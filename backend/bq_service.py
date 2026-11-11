"""
BigQuery Client, Query Cache, Read/Write Repository & Unified Batch Runner
==========================================================================

main.py (FastAPI) と batch_process.py (CLI) で共通利用する BigQuery 操作・
インメモリ TTL キャッシュ・バルクバッチ処理を提供する。
"""

import io
import json
import logging
import math
import os
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

from xbrl_parser import download_xbrl_with_retry, extract_xbrl_data

logger = logging.getLogger(__name__)

# BigQuery テーブル定数
BQ_DATASET = "edinet"
BQ_DOCUMENTS = "documents"
BQ_COMPANY_MASTER = "company_master"
BQ_FINANCIAL_SUMMARY = "financial_summary"
BQ_SEGMENT_DATA = "segment_data"

# バルク書き込みのチャンクサイズ
FLUSH_CHUNK_SIZE = 25

# クエリ並列実行用の共有スレッドプール（リクエスト毎の生成コストを削減）
_QUERY_EXECUTOR = ThreadPoolExecutor(max_workers=6)


# ---------------------------------------------------------------------------
# Pure-Python 値変換ヘルパー（Pandas 非依存で高速化）
# ---------------------------------------------------------------------------


def is_null(val: Any) -> bool:
    """None, NaN, NaT, pd.NA 等を高速に判定する。"""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    # NaN / pd.NA / pd.NaT は自己比較 `val != val` が True または例外になる性質を利用
    try:
        return bool(val != val)
    except Exception:
        return False


def str_or_none(val: Any) -> Optional[str]:
    """値を str に変換する（NULL 相当の場合は None）。"""
    if is_null(val):
        return None
    s = str(val).strip()
    return s if s else None


def int_or_none(val: Any) -> Optional[int]:
    """値を int に変換する（NULL・変換不可の場合は None）。"""
    if is_null(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def float_or_none(val: Any) -> Optional[float]:
    """値を float に変換する（NULL・変換不可の場合は None）。"""
    if is_null(val):
        return None
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# スレッドセーフ TTL キャッシュ（検索・企業詳細の高速化）
# ---------------------------------------------------------------------------


class _SimpleTTLCache:
    """外部ライブラリ不要の軽量・スレッドセーフな TTL 付き LRU 風キャッシュ。"""

    def __init__(self, maxsize: int = 512, ttl_sec: int = 300):
        self.maxsize = maxsize
        self.ttl_sec = ttl_sec
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, val = entry
            if now >= expires_at:
                del self._store[key]
                return None
            return val

    def set(self, key: str, value: Any) -> None:
        now = time.monotonic()
        with self._lock:
            if len(self._store) >= self.maxsize:
                # 期限切れを削除、それでも満杯なら最古のエントリを削除
                expired = [k for k, (exp, _) in self._store.items() if now >= exp]
                for k in expired:
                    del self._store[k]
                if len(self._store) >= self.maxsize:
                    oldest_key = next(iter(self._store))
                    del self._store[oldest_key]
            self._store[key] = (now + self.ttl_sec, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_search_cache = _SimpleTTLCache(maxsize=512, ttl_sec=300)
_company_detail_cache = _SimpleTTLCache(maxsize=256, ttl_sec=600)


def clear_query_cache() -> None:
    """バッチ処理やマスタ更新時にインメモリキャッシュをクリアする。"""
    _search_cache.clear()
    _company_detail_cache.clear()


# ---------------------------------------------------------------------------
# BigQuery Client 初期化（共通化）
# ---------------------------------------------------------------------------

_bq_client: Optional[bigquery.Client] = None


def create_bq_client(credentials_json: Optional[str] = None) -> bigquery.Client:
    """環境変数・引数・ローカルファイル・ADC から BigQuery クライアントを生成する。"""
    # 1. 引数または環境変数の JSON 文字列
    cred_json = (
        credentials_json
        or os.environ.get("GOOGLE_CREDENTIALS_JSON")
        or os.environ.get("GCP_CREDENTIALS_JSON")
    )
    if cred_json:
        info = json.loads(cred_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

    # 2. GOOGLE_APPLICATION_CREDENTIALS (JSON 文字列またはファイルパス)
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    if cred_path.strip().startswith("{"):
        info = json.loads(cred_path)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

    if not os.path.exists(cred_path):
        for candidate in [
            os.path.join(os.path.dirname(__file__), cred_path),
            os.path.join(os.path.dirname(__file__), "..", "credentials.json"),
        ]:
            if os.path.exists(candidate):
                cred_path = candidate
                break

    if os.path.exists(cred_path):
        credentials = service_account.Credentials.from_service_account_file(
            cred_path
        )
        return bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

    # 3. Application Default Credentials (ADC)
    return bigquery.Client()


def get_bq_client() -> bigquery.Client:
    """シングルトン BigQuery クライアントを取得する。"""
    global _bq_client
    if _bq_client is None:
        _bq_client = create_bq_client()
    return _bq_client


# ---------------------------------------------------------------------------
# Read Queries（脱 DataFrame・並列クエリ・キャッシュ対応）
# ---------------------------------------------------------------------------


def search_companies(client: bigquery.Client, q: str) -> list[dict]:
    """企業マスタを検索する（TTL キャッシュ付き・DataFrame 非経由）。"""
    normalized_q = unicodedata.normalize("NFKC", q.strip())
    if not normalized_q:
        return []

    cache_key = normalized_q.lower()
    cached = _search_cache.get(cache_key)
    if cached is not None:
        return cached

    query = f"""
    SELECT
        edinet_code, ticker_symbol, company_name_ja, company_name_en,
        industry, jcn, latest_period_end
    FROM `{BQ_DATASET}.{BQ_COMPANY_MASTER}`
    WHERE
        ticker_symbol = @q
        OR UPPER(edinet_code) = UPPER(@q)
        OR LOWER(company_name_ja) LIKE LOWER(@q_like)
        OR LOWER(company_name_en) LIKE LOWER(@q_like)
    ORDER BY
        CASE
            WHEN ticker_symbol = @q OR UPPER(edinet_code) = UPPER(@q) THEN 0
            WHEN LOWER(company_name_ja) LIKE LOWER(@q_prefix) THEN 1
            ELSE 2
        END,
        company_name_ja
    LIMIT 50
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("q", "STRING", normalized_q),
            bigquery.ScalarQueryParameter("q_like", "STRING", f"%{normalized_q}%"),
            bigquery.ScalarQueryParameter("q_prefix", "STRING", f"{normalized_q}%"),
        ]
    )

    rows = client.query(query, job_config=job_config).result()
    results = [
        {
            "edinet_code": str_or_none(row.get("edinet_code")),
            "ticker_symbol": str_or_none(row.get("ticker_symbol")),
            "company_name_ja": str_or_none(row.get("company_name_ja")),
            "company_name_en": str_or_none(row.get("company_name_en")),
            "industry": str_or_none(row.get("industry")),
            "jcn": str_or_none(row.get("jcn")),
            "latest_period_end": str_or_none(row.get("latest_period_end")),
        }
        for row in rows
    ]

    _search_cache.set(cache_key, results)
    return results


def fetch_company_detail(
    client: bigquery.Client, edinet_code: str
) -> Optional[dict]:
    """
    企業詳細（マスタ・財務サマリ・セグメント）を3クエリ並列実行で取得する。
    存在しない edinet_code の場合は None を返す。
    """
    code = edinet_code.strip()
    cached = _company_detail_cache.get(code)
    if cached is not None:
        return cached

    query_param = [bigquery.ScalarQueryParameter("code", "STRING", code)]

    master_query = f"""
    SELECT
        edinet_code, ticker_symbol, company_name_ja, company_name_en,
        industry, jcn, latest_period_end
    FROM `{BQ_DATASET}.{BQ_COMPANY_MASTER}`
    WHERE edinet_code = @code
    LIMIT 1
    """

    fin_query = f"""
    SELECT
        fiscal_year, period_start, period_end, doc_id, submit_date,
        accounting_standard, revenue, net_income_loss, total_assets,
        net_assets, roe, pbr, per
    FROM `{BQ_DATASET}.{BQ_FINANCIAL_SUMMARY}`
    WHERE edinet_code = @code
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fiscal_year
        ORDER BY submit_date DESC, doc_id DESC
    ) = 1
    ORDER BY period_end DESC
    """

    seg_query = f"""
    SELECT
        fiscal_year, segment_name, segment_revenue,
        segment_profit, segment_employees
    FROM `{BQ_DATASET}.{BQ_SEGMENT_DATA}`
    WHERE edinet_code = @code
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fiscal_year, segment_name
        ORDER BY doc_id DESC
    ) = 1
    ORDER BY fiscal_year DESC, segment_name
    """

    def _run_query(sql: str):
        cfg = bigquery.QueryJobConfig(query_parameters=query_param)
        return list(client.query(sql, job_config=cfg).result())

    fut_master = _QUERY_EXECUTOR.submit(_run_query, master_query)
    fut_fin = _QUERY_EXECUTOR.submit(_run_query, fin_query)
    fut_seg = _QUERY_EXECUTOR.submit(_run_query, seg_query)

    master_rows = fut_master.result()
    fin_rows = fut_fin.result()
    seg_rows = fut_seg.result()

    if not master_rows:
        return None

    master = master_rows[0]
    company_info = {
        "edinet_code": str_or_none(master.get("edinet_code")),
        "ticker_symbol": str_or_none(master.get("ticker_symbol")),
        "company_name_ja": str_or_none(master.get("company_name_ja")),
        "company_name_en": str_or_none(master.get("company_name_en")),
        "industry": str_or_none(master.get("industry")),
        "jcn": str_or_none(master.get("jcn")),
        "latest_period_end": str_or_none(master.get("latest_period_end")),
    }

    financials = [
        {
            "fiscal_year": int_or_none(r.get("fiscal_year")),
            "period_start": str_or_none(r.get("period_start")),
            "period_end": str_or_none(r.get("period_end")),
            "doc_id": str_or_none(r.get("doc_id")),
            "submit_date": str_or_none(r.get("submit_date")),
            "accounting_standard": str_or_none(r.get("accounting_standard")),
            "revenue": int_or_none(r.get("revenue")),
            "net_income_loss": int_or_none(r.get("net_income_loss")),
            "total_assets": int_or_none(r.get("total_assets")),
            "net_assets": int_or_none(r.get("net_assets")),
            "roe": float_or_none(r.get("roe")),
            "pbr": float_or_none(r.get("pbr")),
            "per": float_or_none(r.get("per")),
        }
        for r in fin_rows
    ]

    segments: dict[int, list] = {}
    for r in seg_rows:
        fy = int_or_none(r.get("fiscal_year")) or 0
        if fy not in segments:
            segments[fy] = []
        segments[fy].append(
            {
                "segment_name": str_or_none(r.get("segment_name")),
                "segment_revenue": int_or_none(r.get("segment_revenue")),
                "segment_profit": int_or_none(r.get("segment_profit")),
                "segment_employees": int_or_none(r.get("segment_employees")),
            }
        )

    result = {
        "company": company_info,
        "financials": financials,
        "segments": segments,
    }
    _company_detail_cache.set(code, result)
    return result


# ---------------------------------------------------------------------------
# Write & Bulk Helpers
# ---------------------------------------------------------------------------


def delete_existing_docs(
    client: bigquery.Client, project_id: str, table_id: str, doc_ids: list[str]
) -> None:
    """再実行時の重複防止のため、指定された doc_ids の既存行を削除する。"""
    if not doc_ids:
        return

    query = f"""
    DELETE FROM `{project_id}.{BQ_DATASET}.{table_id}`
    WHERE doc_id IN UNNEST(@doc_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "doc_ids", "STRING", list(set(doc_ids))
            ),
        ]
    )
    client.query(query, job_config=job_config).result()


def insert_rows_load_job(
    client: bigquery.Client, project_id: str, table_id: str, rows: list[dict]
) -> None:
    """NDJSON Load Job を使って BigQuery に複数行を一括挿入する。"""
    if not rows:
        return

    ndjson = "\n".join(json.dumps(row) for row in rows)
    ndjson_bytes = ndjson.encode("utf-8")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    table_ref = f"{project_id}.{BQ_DATASET}.{table_id}"
    load_job = client.load_table_from_file(
        io.BytesIO(ndjson_bytes),
        table_ref,
        job_config=job_config,
    )
    load_job.result()


def mark_documents_processed_bulk(
    client: bigquery.Client, project_id: str, doc_ids: list[str]
) -> None:
    """documents テーブルの複数 doc_id を1クエリで処理済みに更新する。"""
    if not doc_ids:
        return

    query = f"""
    UPDATE `{project_id}.{BQ_DATASET}.{BQ_DOCUMENTS}`
    SET is_processed = TRUE
    WHERE doc_id IN UNNEST(@doc_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "doc_ids", "STRING", list(set(doc_ids))
            ),
        ]
    )
    client.query(query, job_config=job_config).result()


def mark_document_processed(
    client: bigquery.Client, project_id: str, doc_id: str
) -> None:
    """単一 doc_id を処理済みに更新する。"""
    mark_documents_processed_bulk(client, project_id, [doc_id])


def upsert_company_master(
    client: bigquery.Client, project_id: str, edinet_code: str, **kwargs
) -> None:
    """単一企業の company_master を MERGE 更新する（latest_period_end の巻き戻り防止付き）。"""
    clean_kwargs = {k: v for k, v in kwargs.items() if not is_null(v)}
    if not clean_kwargs:
        return

    set_parts = []
    params = [
        bigquery.ScalarQueryParameter("edinet_code", "STRING", edinet_code)
    ]

    for key, value in clean_kwargs.items():
        param_name = f"p_{key}"
        if key == "latest_period_end":
            set_parts.append(
                f"T.{key} = CASE "
                f"WHEN T.{key} IS NULL THEN @{param_name} "
                f"ELSE GREATEST(T.{key}, @{param_name}) END"
            )
            params.append(bigquery.ScalarQueryParameter(param_name, "DATE", value))
        else:
            set_parts.append(f"T.{key} = @{param_name}")
            params.append(
                bigquery.ScalarQueryParameter(param_name, "STRING", str(value))
            )

    set_clause = ", ".join(set_parts)
    now_clause = "T.last_modified = CURRENT_TIMESTAMP()"

    query = f"""
    MERGE `{project_id}.{BQ_DATASET}.{BQ_COMPANY_MASTER}` T
    USING (SELECT @edinet_code AS edinet_code) S
    ON T.edinet_code = S.edinet_code
    WHEN MATCHED THEN
        UPDATE SET {set_clause}, {now_clause}
    WHEN NOT MATCHED THEN
        INSERT (edinet_code, {', '.join(clean_kwargs.keys())}, last_modified)
        VALUES (@edinet_code, {', '.join('@p_' + k for k in clean_kwargs.keys())}, CURRENT_TIMESTAMP())
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(query, job_config=job_config).result()


def upsert_company_master_bulk(
    client: bigquery.Client, project_id: str, company_updates: dict[str, dict]
) -> None:
    """複数企業の company_master を1回の MERGE クエリで一括更新する。"""
    if not company_updates:
        return

    rows = list(company_updates.values())
    json_data = json.dumps(rows)

    query = f"""
    MERGE `{project_id}.{BQ_DATASET}.{BQ_COMPANY_MASTER}` T
    USING (
      SELECT
        JSON_VALUE(j, '$.edinet_code') AS edinet_code,
        JSON_VALUE(j, '$.ticker_symbol') AS ticker_symbol,
        JSON_VALUE(j, '$.company_name_ja') AS company_name_ja,
        JSON_VALUE(j, '$.jcn') AS jcn,
        SAFE_CAST(JSON_VALUE(j, '$.latest_period_end') AS DATE) AS latest_period_end
      FROM UNNEST(JSON_QUERY_ARRAY(@json_str)) AS j
    ) S
    ON T.edinet_code = S.edinet_code
    WHEN MATCHED THEN
      UPDATE SET
        company_name_ja = COALESCE(S.company_name_ja, T.company_name_ja),
        ticker_symbol = COALESCE(S.ticker_symbol, T.ticker_symbol),
        jcn = COALESCE(S.jcn, T.jcn),
        latest_period_end = CASE
          WHEN T.latest_period_end IS NULL THEN S.latest_period_end
          WHEN S.latest_period_end IS NULL THEN T.latest_period_end
          ELSE GREATEST(T.latest_period_end, S.latest_period_end)
        END,
        last_modified = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (edinet_code, ticker_symbol, company_name_ja, jcn, latest_period_end, last_modified)
      VALUES (S.edinet_code, S.ticker_symbol, S.company_name_ja, S.jcn, S.latest_period_end, CURRENT_TIMESTAMP())
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("json_str", "STRING", json_data),
        ]
    )
    client.query(query, job_config=job_config).result()


def flush_batch_buffers(
    client: bigquery.Client,
    project_id: str,
    fin_rows: list[dict],
    seg_rows: list[dict],
    company_updates: dict[str, dict],
    processed_doc_ids: list[str],
) -> None:
    """バッファに溜まったデータを BigQuery に一括書き込みし、バッファをクリアする。"""
    if fin_rows:
        fin_doc_ids = [r["doc_id"] for r in fin_rows if r.get("doc_id")]
        delete_existing_docs(
            client, project_id, BQ_FINANCIAL_SUMMARY, fin_doc_ids
        )
        insert_rows_load_job(
            client, project_id, BQ_FINANCIAL_SUMMARY, fin_rows
        )
        fin_rows.clear()

    if seg_rows:
        seg_doc_ids = list({r["doc_id"] for r in seg_rows if r.get("doc_id")})
        delete_existing_docs(client, project_id, BQ_SEGMENT_DATA, seg_doc_ids)
        insert_rows_load_job(client, project_id, BQ_SEGMENT_DATA, seg_rows)
        seg_rows.clear()

    if company_updates:
        upsert_company_master_bulk(client, project_id, company_updates)
        company_updates.clear()

    if processed_doc_ids:
        mark_documents_processed_bulk(client, project_id, processed_doc_ids)
        processed_doc_ids.clear()


# ---------------------------------------------------------------------------
# Unified Batch Processor（API / CLI 共通）
# ---------------------------------------------------------------------------


def execute_batch(
    client: bigquery.Client,
    api_key: str,
    limit: int = 100,
    order: str = "desc",
) -> dict:
    """
    未処理の documents を取得し、XBRL ダウンロード → パース → BigQuery バルク投入を実行する。
    FastAPI エンドポイントと CLI バッチスクリプトの両方から共通利用される。
    """
    project_id = client.project
    order_clause = "DESC" if order == "desc" else "ASC"

    query = f"""
    SELECT doc_id, edinet_code, sec_code, filer_name, jcn,
           period_start, period_end, submit_date_time
    FROM `{project_id}.{BQ_DATASET}.{BQ_DOCUMENTS}`
    WHERE (is_processed IS NULL OR is_processed = FALSE)
      AND edinet_code IS NOT NULL
      AND edinet_code != ''
    ORDER BY submit_date_time {order_clause}
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INTEGER", limit)
        ]
    )
    doc_rows = list(client.query(query, job_config=job_config).result())
    total_found = len(doc_rows)

    if total_found == 0:
        logger.info("✅ No unprocessed documents found. Nothing to do.")
        return {
            "processed": 0,
            "skipped": 0,
            "errors": [],
            "total_found": 0,
            "message": "No unprocessed documents found.",
        }

    logger.info(
        "📋 Found %d unprocessed documents (order=%s, limit=%d)",
        total_found,
        order,
        limit,
    )

    processed_count = 0
    skipped_count = 0
    errors: list[dict] = []

    fin_rows_buf: list[dict] = []
    seg_rows_buf: list[dict] = []
    company_updates_buf: dict[str, dict] = {}
    processed_doc_ids_buf: list[str] = []

    for idx, doc_row in enumerate(doc_rows, start=1):
        doc_id = doc_row["doc_id"]
        edinet_code = doc_row["edinet_code"]
        logger.info(
            "[%d/%d] Processing doc_id=%s (edinet_code=%s)",
            idx,
            total_found,
            doc_id,
            edinet_code,
        )

        try:
            xbrl_content, lab_content, def_content = download_xbrl_with_retry(
                doc_id, api_key
            )

            if not xbrl_content or not lab_content:
                logger.info(
                    "  No XBRL content for %s. Buffering as processed.", doc_id
                )
                processed_doc_ids_buf.append(doc_id)
                skipped_count += 1
                if len(processed_doc_ids_buf) >= FLUSH_CHUNK_SIZE:
                    logger.info(
                        "  💾 Flushing batch chunk (%d docs)...",
                        len(processed_doc_ids_buf),
                    )
                    flush_batch_buffers(
                        client,
                        project_id,
                        fin_rows_buf,
                        seg_rows_buf,
                        company_updates_buf,
                        processed_doc_ids_buf,
                    )
                time.sleep(1)
                continue

            company_summary, segment_details, _ = extract_xbrl_data(
                xbrl_content, lab_content, def_content
            )

            period_end_str = str_or_none(doc_row.get("period_end"))
            fiscal_year = int(period_end_str[:4]) if period_end_str else None

            fin_rows_buf.append(
                {
                    "edinet_code": edinet_code,
                    "fiscal_year": fiscal_year,
                    "period_start": str_or_none(doc_row.get("period_start")),
                    "period_end": period_end_str,
                    "doc_id": doc_id,
                    "submit_date": str_or_none(
                        doc_row.get("submit_date_time")
                    ),
                    "accounting_standard": str_or_none(
                        company_summary.get("accounting_standard")
                    ),
                    "revenue": int_or_none(company_summary.get("revenue")),
                    "net_income_loss": int_or_none(
                        company_summary.get("net_income_loss")
                    ),
                    "total_assets": int_or_none(
                        company_summary.get("total_assets")
                    ),
                    "net_assets": int_or_none(
                        company_summary.get("net_assets")
                    ),
                    "roe": float_or_none(company_summary.get("roe")),
                    "pbr": float_or_none(company_summary.get("pbr")),
                    "per": float_or_none(company_summary.get("per")),
                }
            )

            if segment_details:
                for seg in segment_details:
                    seg_rows_buf.append(
                        {
                            "edinet_code": edinet_code,
                            "fiscal_year": fiscal_year,
                            "doc_id": doc_id,
                            "segment_name": seg["segment_name"],
                            "segment_revenue": int_or_none(
                                seg.get("segment_revenue")
                            ),
                            "segment_profit": int_or_none(
                                seg.get("segment_profit")
                            ),
                            "segment_employees": int_or_none(
                                seg.get("segment_employees")
                            ),
                        }
                    )

            sec_code_str = str_or_none(doc_row.get("sec_code"))
            ticker_symbol = (
                sec_code_str[:4]
                if sec_code_str and len(sec_code_str) >= 4
                else None
            )
            filer_name = str_or_none(doc_row.get("filer_name"))
            jcn = str_or_none(doc_row.get("jcn"))

            existing_comp = company_updates_buf.get(edinet_code)
            if not existing_comp:
                company_updates_buf[edinet_code] = {
                    "edinet_code": edinet_code,
                    "ticker_symbol": ticker_symbol,
                    "company_name_ja": filer_name,
                    "jcn": jcn,
                    "latest_period_end": period_end_str,
                }
            else:
                if ticker_symbol and not existing_comp.get("ticker_symbol"):
                    existing_comp["ticker_symbol"] = ticker_symbol
                if filer_name and not existing_comp.get("company_name_ja"):
                    existing_comp["company_name_ja"] = filer_name
                if jcn and not existing_comp.get("jcn"):
                    existing_comp["jcn"] = jcn
                if period_end_str:
                    prev_end = existing_comp.get("latest_period_end")
                    if not prev_end or period_end_str > prev_end:
                        existing_comp["latest_period_end"] = period_end_str

            processed_doc_ids_buf.append(doc_id)
            processed_count += 1
            logger.info("  ✅ Parsed: %s", doc_id)

            if len(processed_doc_ids_buf) >= FLUSH_CHUNK_SIZE:
                logger.info(
                    "  💾 Flushing batch chunk (%d docs)...",
                    len(processed_doc_ids_buf),
                )
                flush_batch_buffers(
                    client,
                    project_id,
                    fin_rows_buf,
                    seg_rows_buf,
                    company_updates_buf,
                    processed_doc_ids_buf,
                )

            time.sleep(1)

        except Exception as e:
            logger.error("  ❌ Error processing %s: %s", doc_id, str(e))
            errors.append({"doc_id": doc_id, "error": str(e)})
            continue

    if (
        processed_doc_ids_buf
        or fin_rows_buf
        or seg_rows_buf
        or company_updates_buf
    ):
        logger.info(
            "  💾 Flushing final batch chunk (%d docs)...",
            len(processed_doc_ids_buf),
        )
        flush_batch_buffers(
            client,
            project_id,
            fin_rows_buf,
            seg_rows_buf,
            company_updates_buf,
            processed_doc_ids_buf,
        )

    # データ更新に伴いクエリキャッシュをクリア
    clear_query_cache()

    logger.info("=" * 60)
    logger.info(
        "🏁 Batch complete: processed=%d, skipped=%d, errors=%d, total=%d",
        processed_count,
        skipped_count,
        len(errors),
        total_found,
    )
    logger.info("=" * 60)

    return {
        "processed": processed_count,
        "skipped": skipped_count,
        "errors": errors,
        "total_found": total_found,
    }

