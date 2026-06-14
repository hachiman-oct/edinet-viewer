"""
EDINET XBRL Batch Processor — CLI スタンドアロンスクリプト

GitHub Actions から直接実行し、BigQuery の documents テーブルから
未処理の有価証券報告書を取得 → XBRL パース → financial_summary /
segment_data / company_master に投入する。

Usage:
    python batch_process.py --limit 100 --order desc
    python batch_process.py --limit 200 --order asc --credentials-json '{"type":"service_account",...}'
"""

import argparse
import io
import json
import logging
import os
import sys
import time
import zipfile

import requests as http_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ── main.py からビジネスロジックを import ──────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from taxonomy import COMPANY_TAGS, SEGMENT_TAGS

# main.py の XBRL パース関数群を import
from main import extract_xbrl_data, parse_num

# ── ログ設定 ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────────────
BQ_DATASET = "edinet"
BQ_DOCUMENTS = "documents"
BQ_COMPANY_MASTER = "company_master"
BQ_FINANCIAL_SUMMARY = "financial_summary"
BQ_SEGMENT_DATA = "segment_data"

EDINET_API_DOC_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}?type=1&Subscription-Key={api_key}"

# リトライ設定
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5  # 秒（指数バックオフ: 5, 10, 20）
RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


# ── BigQuery クライアント初期化 ────────────────────────────────
def create_bq_client(credentials_json: str | None = None) -> bigquery.Client:
    """BigQuery クライアントを作成する。"""
    if credentials_json:
        info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    # ローカル実行時: credentials.json ファイルを探す
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    if not os.path.exists(cred_path):
        alt_path = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
        if os.path.exists(alt_path):
            cred_path = alt_path

    if os.path.exists(cred_path):
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    return bigquery.Client()


# ── EDINET API ダウンロード（リトライ付き） ────────────────────
def download_xbrl_with_retry(doc_id: str, api_key: str) -> tuple[bytes | None, bytes | None, bytes | None]:
    """
    EDINET API から XBRL ZIP をダウンロードし、xbrl / lab / def を抽出する。
    500/503 等の一時エラーには指数バックオフでリトライする。
    """
    url = EDINET_API_DOC_URL.format(doc_id=doc_id, api_key=api_key)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http_requests.get(url, timeout=60)

            if resp.status_code in RETRYABLE_STATUS_CODES:
                wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning(
                    "  EDINET API returned %d for %s (attempt %d/%d). Retrying in %ds...",
                    resp.status_code, doc_id, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logger.error(
                    "  EDINET API returned %d for %s (non-retryable). Skipping.",
                    resp.status_code, doc_id,
                )
                return None, None, None

            # ZIP 解凍
            try:
                z = zipfile.ZipFile(io.BytesIO(resp.content))
            except zipfile.BadZipFile:
                logger.warning("  Downloaded file for %s is not a valid ZIP. Skipping.", doc_id)
                return None, None, None

            xbrl_content = None
            lab_content = None
            def_content = None

            xbrl_file = next(
                (f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith(".xbrl")), None
            )
            if xbrl_file:
                xbrl_content = z.read(xbrl_file)
            else:
                htm_file = next(
                    (f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith(".htm")), None
                )
                if htm_file:
                    xbrl_content = z.read(htm_file)

            lab_file = next(
                (f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith("_lab.xml")), None
            )
            if lab_file:
                lab_content = z.read(lab_file)

            def_file = next(
                (f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith("_def.xml")), None
            )
            if def_file:
                def_content = z.read(def_file)

            return xbrl_content, lab_content, def_content

        except http_requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning(
                "  Network error for %s (attempt %d/%d): %s. Retrying in %ds...",
                doc_id, attempt, MAX_RETRIES, str(e), wait,
            )
            time.sleep(wait)

    logger.error("  All %d retries exhausted for %s. Skipping.", MAX_RETRIES, doc_id)
    return None, None, None


# ── BigQuery 書き込みヘルパー ──────────────────────────────────
def insert_rows_load_job(client: bigquery.Client, project_id: str, table_id: str, rows: list[dict]):
    """Load API を使って BigQuery に行を挿入する。"""
    import pandas as pd

    table_ref = f"{project_id}.{BQ_DATASET}.{table_id}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()


def mark_document_processed(client: bigquery.Client, project_id: str, doc_id: str):
    """documents テーブルの is_processed を TRUE に更新する。"""
    query = f"""
    UPDATE `{project_id}.{BQ_DATASET}.{BQ_DOCUMENTS}`
    SET is_processed = TRUE
    WHERE doc_id = @doc_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("doc_id", "STRING", doc_id)]
    )
    client.query(query, job_config=job_config).result()


def upsert_company_master(client: bigquery.Client, project_id: str, edinet_code: str, **kwargs):
    """company_master に MERGE (UPSERT) する。"""
    set_parts = []
    params = [bigquery.ScalarQueryParameter("edinet_code", "STRING", edinet_code)]

    for key, value in kwargs.items():
        if value is not None:
            param_name = f"p_{key}"
            set_parts.append(f"T.{key} = @{param_name}")
            if key == "latest_period_end":
                params.append(bigquery.ScalarQueryParameter(param_name, "DATE", value))
            else:
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

    if not set_parts:
        return

    set_clause = ", ".join(set_parts)
    now_clause = "T.last_modified = CURRENT_TIMESTAMP()"

    query = f"""
    MERGE `{project_id}.{BQ_DATASET}.{BQ_COMPANY_MASTER}` T
    USING (SELECT @edinet_code AS edinet_code) S
    ON T.edinet_code = S.edinet_code
    WHEN MATCHED THEN
        UPDATE SET {set_clause}, {now_clause}
    WHEN NOT MATCHED THEN
        INSERT (edinet_code, {', '.join(kwargs.keys())}, last_modified)
        VALUES (@edinet_code, {', '.join('@p_' + k for k in kwargs.keys())}, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(query, job_config=job_config).result()


# ── メイン処理 ─────────────────────────────────────────────────
def run_batch(client: bigquery.Client, api_key: str, limit: int, order: str):
    """
    未処理の documents を取得し、XBRL パース → BigQuery に投入する。

    Args:
        client: BigQuery クライアント
        api_key: EDINET API キー
        limit: 1回の実行で処理する最大件数
        order: "desc"（新しい順 = 日次）or "asc"（古い順 = バックフィル）
    """
    import pandas as pd

    project_id = client.project
    order_clause = "DESC" if order == "desc" else "ASC"

    # 1. 未処理ドキュメントを取得
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
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INTEGER", limit)]
    )
    df_docs = client.query(query, job_config=job_config).to_dataframe()

    total_found = len(df_docs)
    if total_found == 0:
        logger.info("✅ No unprocessed documents found. Nothing to do.")
        return

    logger.info("📋 Found %d unprocessed documents (order=%s, limit=%d)", total_found, order, limit)

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for idx, doc_row in df_docs.iterrows():
        doc_id = doc_row["doc_id"]
        edinet_code = doc_row["edinet_code"]
        progress = f"[{processed_count + skipped_count + error_count + 1}/{total_found}]"

        logger.info("%s Processing doc_id=%s (edinet_code=%s)", progress, doc_id, edinet_code)

        try:
            # 2. XBRL ダウンロード（リトライ付き）
            xbrl_content, lab_content, def_content = download_xbrl_with_retry(doc_id, api_key)

            if not xbrl_content or not lab_content:
                logger.info("  No XBRL content for %s. Marking as processed.", doc_id)
                mark_document_processed(client, project_id, doc_id)
                skipped_count += 1
                time.sleep(1)
                continue

            # 3. XBRL パース
            company_summary, segment_details, logs = extract_xbrl_data(
                xbrl_content, lab_content, def_content
            )

            # 4. fiscal_year 算出
            period_end = doc_row.get("period_end")
            fiscal_year = None
            if pd.notnull(period_end):
                fiscal_year = int(str(period_end)[:4])

            # 5. financial_summary に挿入
            fin_row = {
                "edinet_code": edinet_code,
                "fiscal_year": fiscal_year,
                "period_start": str(doc_row["period_start"]) if pd.notnull(doc_row.get("period_start")) else None,
                "period_end": str(period_end) if pd.notnull(period_end) else None,
                "doc_id": doc_id,
                "submit_date": str(doc_row["submit_date_time"]) if pd.notnull(doc_row.get("submit_date_time")) else None,
                "accounting_standard": str(company_summary.get("accounting_standard")) if company_summary.get("accounting_standard") else None,
                "revenue": company_summary.get("revenue"),
                "net_income_loss": company_summary.get("net_income_loss"),
                "total_assets": company_summary.get("total_assets"),
                "net_assets": company_summary.get("net_assets"),
                "roe": company_summary.get("roe"),
                "pbr": company_summary.get("pbr"),
                "per": company_summary.get("per"),
            }
            insert_rows_load_job(client, project_id, BQ_FINANCIAL_SUMMARY, [fin_row])

            # 6. segment_data に挿入
            if segment_details:
                seg_rows = []
                for seg in segment_details:
                    seg_rows.append({
                        "edinet_code": edinet_code,
                        "fiscal_year": fiscal_year,
                        "doc_id": doc_id,
                        "segment_name": seg["segment_name"],
                        "segment_revenue": seg.get("segment_revenue"),
                        "segment_profit": seg.get("segment_profit"),
                        "segment_employees": seg.get("segment_employees"),
                    })
                insert_rows_load_job(client, project_id, BQ_SEGMENT_DATA, seg_rows)

            # 7. company_master を UPSERT
            upsert_company_master(
                client, project_id, edinet_code,
                company_name_ja=doc_row.get("filer_name"),
                jcn=doc_row.get("jcn"),
                latest_period_end=str(period_end) if pd.notnull(period_end) else None,
            )

            # 8. 処理済みマーク
            mark_document_processed(client, project_id, doc_id)
            processed_count += 1
            logger.info("  ✅ Done: %s", doc_id)

            # EDINET API レート制限対策
            time.sleep(1)

        except Exception as e:
            error_count += 1
            logger.error("  ❌ Error processing %s: %s", doc_id, str(e))
            # エラーが発生しても次の書類に進む
            continue

    # ── サマリー出力 ──
    logger.info("=" * 60)
    logger.info("🏁 Batch complete: processed=%d, skipped=%d, errors=%d, total=%d",
                processed_count, skipped_count, error_count, total_found)
    logger.info("=" * 60)


# ── CLI エントリーポイント ─────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="EDINET XBRL Batch Processor")
    parser.add_argument("--limit", type=int, default=100, help="Max documents to process per run")
    parser.add_argument("--order", choices=["asc", "desc"], default="desc",
                        help="Processing order: 'desc' (newest first) or 'asc' (oldest first)")
    parser.add_argument("--credentials-json", type=str, default=None,
                        help="GCP service account JSON string (for CI/CD)")
    args = parser.parse_args()

    # .env を読み込み（ローカル実行時）
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    api_key = os.environ.get("EDINET_API_KEY", "")
    if not api_key:
        logger.error("EDINET_API_KEY is not set. Set it in .env or as an environment variable.")
        sys.exit(1)

    client = create_bq_client(args.credentials_json)
    logger.info("🚀 Starting batch process (limit=%d, order=%s, project=%s)",
                args.limit, args.order, client.project)

    run_batch(client, api_key, args.limit, args.order)


if __name__ == "__main__":
    main()
