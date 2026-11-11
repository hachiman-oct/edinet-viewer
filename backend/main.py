"""
EDINET XBRL Viewer — FastAPI Backend (v2)

Architecture:
  - BigQuery に企業マスタ・財務サマリ・セグメントデータを事前格納
  - フロントエンドからはデータ参照のみ (リアルタイム XBRL ダウンロード不要)
  - 検索・企業詳細 API は DataFrame 非経由 + 並列クエリ + インメモリ TTL キャッシュで高速応答
  - バッチ処理・XBRL パース・BigQuery 操作は bq_service.py / xbrl_parser.py に共通化
"""

import csv
import json
import os
import time
from collections import defaultdict
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel

from bq_service import (
    BQ_COMPANY_MASTER,
    BQ_DATASET,
    BQ_DOCUMENTS,
    BQ_FINANCIAL_SUMMARY,
    BQ_SEGMENT_DATA,
    clear_query_cache,
    delete_existing_docs as _delete_existing_docs,
    execute_batch,
    fetch_company_detail,
    float_or_none as _float_or_none,
    flush_batch_buffers as _flush_batch_buffers,
    get_bq_client,
    insert_rows_load_job as _insert_rows_load_job,
    int_or_none as _int_or_none,
    mark_document_processed as _mark_document_processed,
    mark_documents_processed_bulk as _mark_documents_processed_bulk,
    search_companies,
    str_or_none as _str_or_none,
    upsert_company_master as _upsert_company_master,
    upsert_company_master_bulk as _upsert_company_master_bulk,
)
from xbrl_parser import (
    download_xbrl_with_retry,
    extract_xbrl_data,
    parse_num,
)

# Load environment variables (from backend/.env regardless of cwd)
_env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(_env_path)

# EDINET API Key from environment
EDINET_API_KEY = os.environ.get("EDINET_API_KEY", "")


# ---------------------------------------------------------------------------
# Rate Limiter (in-memory, per-IP sliding window)
# ---------------------------------------------------------------------------

RATE_LIMIT_MAX_REQUESTS = 5   # max requests per window
RATE_LIMIT_WINDOW_SEC = 60     # sliding window in seconds

_rate_limit_store: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if the client has exceeded the rate limit."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW_SEC

    timestamps = _rate_limit_store[client_ip]
    _rate_limit_store[client_ip] = [t for t in timestamps if t > window_start]

    if len(_rate_limit_store[client_ip]) >= RATE_LIMIT_MAX_REQUESTS:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Rate limit exceeded. Maximum {RATE_LIMIT_MAX_REQUESTS} "
                f"requests per {RATE_LIMIT_WINDOW_SEC} seconds. "
                "Please wait and try again."
            ),
        )

    _rate_limit_store[client_ip].append(now)


app = FastAPI(
    title="EDINET XBRL Viewer API",
    description="Search companies in BigQuery, view financial summaries and segment data.",
    version="2.0.0",
)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://edinet-viewer.vercel.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_project_id() -> str:
    """Get the project ID from the BigQuery client."""
    return get_bq_client().project


def download_and_extract_xbrl(doc_id: str, api_key: str):
    """後方互換ラッパー: EDINET API から XBRL ZIP をダウンロード・展開する。"""
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail="EDINET API Key is not configured. Set EDINET_API_KEY in your .env file.",
        )
    return download_xbrl_with_retry(doc_id, api_key)


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class BatchProcessRequest(BaseModel):
    limit: int = 10  # max documents to process per batch call
    api_key: Optional[str] = None  # Optional: uses server-side EDINET_API_KEY if omitted


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/search")
def api_search(
    response: Response,
    q: str = Query(..., description="検索クエリ（企業名日英、銘柄コード、証券コード）"),
):
    """Search companies in BigQuery company_master (with TTL cache)."""
    client = get_bq_client()
    results = search_companies(client, q)
    response.headers["Cache-Control"] = (
        "public, max-age=300, stale-while-revalidate=600"
    )
    return {"results": results}


@app.get("/api/companies/{edinet_code}")
def api_company_detail(edinet_code: str, response: Response):
    """Get company detail: master info + all financial summaries + segment data (parallel & cached)."""
    client = get_bq_client()
    detail = fetch_company_detail(client, edinet_code)
    if detail is None:
        raise HTTPException(
            status_code=404, detail=f"Company not found: {edinet_code}"
        )

    response.headers["Cache-Control"] = (
        "public, max-age=300, stale-while-revalidate=600"
    )
    return detail


@app.post("/api/batch/process")
def api_batch_process(req: BatchProcessRequest, request: Request):
    """
    Batch process: find unprocessed documents in BigQuery,
    download XBRL, parse, and bulk-insert into financial_summary + segment_data.
    Also bulk-upsert company_master and mark documents processed.
    """
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    api_key = req.api_key or EDINET_API_KEY
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail="EDINET API Key is required. Set EDINET_API_KEY in .env or pass api_key.",
        )

    client = get_bq_client()
    return execute_batch(client, api_key=api_key, limit=req.limit, order="desc")


@app.post("/api/import/edinet-codelist")
async def api_import_edinet_codelist(request: Request):
    """
    Import EDINET code list CSV to populate company_master with
    English names, ticker symbols, and industry.
    """
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(
            status_code=400,
            detail="Please upload a CSV file using multipart/form-data.",
        )

    form = await request.form()
    file = form.get("file")
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded.")

    raw_bytes = await file.read()

    # Try Shift-JIS first (EDINET CSV default), fallback to UTF-8
    try:
        text = raw_bytes.decode("cp932")
    except UnicodeDecodeError:
        text = raw_bytes.decode("utf-8")

    lines = text.splitlines()
    if lines and "ダウンロード" in lines[0]:
        lines = lines[1:]

    reader = csv.DictReader(lines)

    client = get_bq_client()
    project_id = _get_project_id()
    rows_by_code: dict[str, dict] = {}

    for csv_row in reader:
        edinet_code = csv_row.get("ＥＤＩＮＥＴコード", "").strip()
        if not edinet_code:
            continue

        sec_code_raw = csv_row.get("証券コード", "").strip()
        ticker_symbol = (
            sec_code_raw[:4]
            if sec_code_raw and len(sec_code_raw) >= 4
            else None
        )

        company_name_ja = csv_row.get("提出者名", "").strip() or None
        company_name_en = csv_row.get("提出者名（英字）", "").strip() or None
        industry = csv_row.get("提出者業種", "").strip() or None
        jcn = csv_row.get("提出者法人番号", "").strip() or None

        rows_by_code[edinet_code] = {
            "edinet_code": edinet_code,
            "ticker_symbol": ticker_symbol,
            "company_name_ja": company_name_ja,
            "company_name_en": company_name_en,
            "industry": industry,
            "jcn": jcn,
        }

    if not rows_by_code:
        return {"imported": 0, "message": "No valid rows found in CSV."}

    rows = list(rows_by_code.values())
    json_data = json.dumps(rows)
    query = f"""
    MERGE `{project_id}.{BQ_DATASET}.{BQ_COMPANY_MASTER}` T
    USING (
      SELECT
        JSON_VALUE(j, '$.edinet_code') AS edinet_code,
        JSON_VALUE(j, '$.ticker_symbol') AS ticker_symbol,
        JSON_VALUE(j, '$.company_name_ja') AS company_name_ja,
        JSON_VALUE(j, '$.company_name_en') AS company_name_en,
        JSON_VALUE(j, '$.industry') AS industry,
        JSON_VALUE(j, '$.jcn') AS jcn
      FROM UNNEST(JSON_QUERY_ARRAY(@json_str)) AS j
    ) S
    ON T.edinet_code = S.edinet_code
    WHEN MATCHED AND (IFNULL(T.ticker_symbol, '') != IFNULL(S.ticker_symbol, '')
                      OR IFNULL(T.company_name_en, '') != IFNULL(S.company_name_en, '')) THEN
      UPDATE SET
        ticker_symbol = COALESCE(S.ticker_symbol, T.ticker_symbol),
        company_name_en = COALESCE(S.company_name_en, T.company_name_en),
        industry = COALESCE(S.industry, T.industry),
        last_modified = CURRENT_TIMESTAMP()
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("json_str", "STRING", json_data),
            ]
        )
        client.query(query, job_config=job_config).result()
        clear_query_cache()
    except Exception as e:
        return {"error": str(e), "message": "Failed to execute MERGE query"}

    return {"imported": len(rows), "message": "Merged successfully"}


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
