"""
EDINET XBRL Viewer — FastAPI Backend (v2)

Architecture:
  - BigQuery に企業マスタ・財務サマリ・セグメントデータを事前格納
  - フロントエンドからはデータ参照のみ (リアルタイム XBRL ダウンロード不要)
  - バッチエンドポイントで documents テーブルの未処理 doc_id を
    XBRL ダウンロード → パース → 新テーブルに投入
"""

import os
import io
import csv
import time
import zipfile
import unicodedata
from collections import defaultdict
from typing import Optional

import requests as http_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from taxonomy import COMPANY_TAGS, SEGMENT_TAGS

# Load environment variables (from backend/.env regardless of cwd)
_env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(_env_path)

# EDINET API Key from environment
EDINET_API_KEY = os.environ.get("EDINET_API_KEY", "")

# BigQuery table names
BQ_DATASET = "edinet"
BQ_DOCUMENTS = "documents"
BQ_COMPANY_MASTER = "company_master"
BQ_FINANCIAL_SUMMARY = "financial_summary"
BQ_SEGMENT_DATA = "segment_data"


# ---------------------------------------------------------------------------
# Rate Limiter (in-memory, per-IP sliding window)
# ---------------------------------------------------------------------------

RATE_LIMIT_MAX_REQUESTS = 5   # max requests per window
RATE_LIMIT_WINDOW_SEC = 60     # sliding window in seconds

# dict[ip_address] -> list of timestamps
_rate_limit_store: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if the client has exceeded the rate limit."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW_SEC

    # Prune old entries
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
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# BigQuery Client
# ---------------------------------------------------------------------------

_bq_client = None


def get_bq_client():
    global _bq_client
    if _bq_client is not None:
        return _bq_client

    from google.cloud import bigquery
    from google.oauth2 import service_account

    # 1. Try local credentials.json (relative to backend/)
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    # Also check one level up (project root) for backward compat
    if not os.path.exists(cred_path):
        alt_path = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
        if os.path.exists(alt_path):
            cred_path = alt_path

    if os.path.exists(cred_path):
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        _bq_client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        return _bq_client

    # 2. Fallback to default (e.g. GOOGLE_APPLICATION_CREDENTIALS env var)
    _bq_client = bigquery.Client()
    return _bq_client


def _get_project_id() -> str:
    """Get the project ID from the BigQuery client."""
    return get_bq_client().project


# ---------------------------------------------------------------------------
# XBRL Processing Functions (from v1)
# ---------------------------------------------------------------------------


def download_and_extract_xbrl(doc_id: str, api_key: str):
    """Download ZIP from EDINET API and extract XBRL + label + definition content."""
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail="EDINET API Key is not configured. Set EDINET_API_KEY in your .env file.",
        )

    url = (
        f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
        f"?type=1&Subscription-Key={api_key}"
    )
    resp = http_requests.get(url)
    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to download document from EDINET API (Status {resp.status_code})",
        )

    try:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        raise HTTPException(
            status_code=422,
            detail="Downloaded file from EDINET is not a valid zip file. The document might not have XBRL attached.",
        )

    xbrl_content = None
    lab_content = None
    def_content = None

    # In EDINET, the .xbrl file contains all the facts in one single file,
    # whereas .htm files are split into multiple files. We should prioritize .xbrl.
    xbrl_filename = next(
        (
            f
            for f in z.namelist()
            if f.startswith("XBRL/PublicDoc/") and f.endswith(".xbrl")
        ),
        None,
    )
    if xbrl_filename:
        xbrl_content = z.read(xbrl_filename)
    else:
        # Fallback to the first .htm if no .xbrl is found (rare)
        htm_filename = next(
            (
                f
                for f in z.namelist()
                if f.startswith("XBRL/PublicDoc/") and f.endswith(".htm")
            ),
            None,
        )
        if htm_filename:
            xbrl_content = z.read(htm_filename)

    lab_filename = next(
        (
            f
            for f in z.namelist()
            if f.startswith("XBRL/PublicDoc/") and f.endswith("_lab.xml")
        ),
        None,
    )
    if lab_filename:
        lab_content = z.read(lab_filename)

    def_filename = next(
        (
            f
            for f in z.namelist()
            if f.startswith("XBRL/PublicDoc/") and f.endswith("_def.xml")
        ),
        None,
    )
    if def_filename:
        def_content = z.read(def_filename)

    return xbrl_content, lab_content, def_content


def parse_num(val):
    """Parse a string into int or float if possible."""
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def extract_xbrl_data(xbrl_content, lab_content, def_content=None):
    """
    Extract company summary and segment details from XBRL/Label/Definition XML.
    Uses def.xml to filter out aggregate members, keeping only leaf-node segments.
    """
    logs = []
    if not xbrl_content or not lab_content:
        logs.append("Error: Missing XBRL or Label Linkbase content.")
        return {}, [], logs

    try:
        import re
        # Decode bytes to string
        xbrl_str = xbrl_content.decode("utf-8") if isinstance(xbrl_content, bytes) else xbrl_content
        # Remove TextBlock elements to speed up parsing and reduce memory
        xbrl_str = re.sub(r'<([a-zA-Z0-9_:]*TextBlock)\b[^>]*>.*?</\1>', '', xbrl_str, flags=re.DOTALL)
        xbrl_str = re.sub(r'<([a-zA-Z0-9_:]*TextBlock)\b[^>]*/>', '', xbrl_str)

        soup_xbrl = BeautifulSoup(xbrl_str, "xml")
        soup_lab = BeautifulSoup(lab_content, "xml")
        logs.append("Successfully parsed XBRL and Label Linkbase XML.")
    except Exception as e:
        logs.append(f"Error parsing XML: {e}")
        return {}, [], logs

    # Build facts index to optimize lookups (O(1) instead of O(N))
    from collections import defaultdict
    facts_by_tag = defaultdict(list)
    for t in soup_xbrl.find_all(True):
        t_name = t.name or ""
        local_name = t_name.rsplit(":", 1)[-1]
        facts_by_tag[local_name].append(t)
        name_attr = t.get("name")
        if name_attr:
            name_local = name_attr.rsplit(":", 1)[-1]
            if name_local != local_name:
                facts_by_tag[name_local].append(t)

    # --- 1. Extract Company Summary ---
    logs.append("\n--- Extracting Company Summary ---")

    def get_first_text(metric_name, tags):
        valid_contexts = [
            "CurrentYearDuration",
            "CurrentYearInstant",
            "FilingDateInstant",
        ]
        for tag_name in tags:
            for t in facts_by_tag.get(tag_name, []):
                ctx = t.get("contextRef", "")
                if ctx in valid_contexts:
                    val = parse_num(t.text.strip())
                    logs.append(
                        f"  -> FOUND {metric_name} ({t.name}): {val} [Context: {ctx}]"
                    )
                    return val
        logs.append(
            f"  -> NO Data found for {metric_name}. Tried tags: {', '.join(tags)}"
        )
        return None

    # Get Period Start from CurrentYearDuration context
    period_start = None
    cy_ctx = soup_xbrl.find("context", id="CurrentYearDuration")
    if cy_ctx:
        period_node = cy_ctx.find("period")
        if period_node and period_node.find("startDate"):
            period_start = period_node.find("startDate").text.strip()

    per_val = get_first_text("PER", COMPANY_TAGS["PER"])
    roe_val = get_first_text("ROE", COMPANY_TAGS["ROE"])
    pbr_val = None
    if isinstance(per_val, (int, float)) and isinstance(roe_val, (int, float)):
        pbr_val = per_val * roe_val

    company_summary = {
        "period_start": period_start,
        "accounting_standard": get_first_text(
            "Accounting Standards", COMPANY_TAGS["Accounting Standards"]
        ),
        "revenue": get_first_text(
            "Net Sales", COMPANY_TAGS["Net Sales"]
        ),
        "net_income_loss": get_first_text(
            "Net Income", COMPANY_TAGS["Net Income"]
        ),
        "total_assets": get_first_text(
            "Total Assets", COMPANY_TAGS["Total Assets"]
        ),
        "net_assets": get_first_text(
            "Net Assets", COMPANY_TAGS["Net Assets"]
        ),
        "per": per_val,
        "pbr": pbr_val,
        "roe": roe_val,
    }

    # --- 2. Extract Segment Details ---
    explicit_members = soup_xbrl.find_all(
        attrs={"dimension": "jpcrp_cor:OperatingSegmentsAxis"}
    )
    logs.append(
        f"Found {len(explicit_members)} elements with dimension='jpcrp_cor:OperatingSegmentsAxis'."
    )

    unique_segments = {}
    for mem in explicit_members:
        text = mem.text.strip()
        if not text:
            continue

        segment_id = text.split(":")[-1]
        unique_segments[segment_id] = {
            "full_text": text,
            "segment_id": segment_id,
            "context_id_dur": "CurrentYearDuration_" + text.replace(":", ""),
            "context_id_inst": "CurrentYearInstant_" + text.replace(":", ""),
        }

    logs.append(f"Extracted {len(unique_segments)} unique segments (before leaf-node filtering).")

    # --- Filter to leaf-node segments using def.xml ---
    if def_content:
        try:
            soup_def = BeautifulSoup(def_content, "xml")

            loc_label_to_member: dict[str, str] = {}
            for loc in soup_def.find_all(["link:loc", "loc"]):
                label = loc.get("xlink:label", "")
                href = loc.get("xlink:href", "")
                if "#" in href:
                    member_name = href.split("#")[-1]
                    loc_label_to_member[label] = member_name

            from_members: set[str] = set()
            to_members: set[str] = set()
            for arc in soup_def.find_all(["link:definitionArc", "definitionArc"]):
                from_label = arc.get("xlink:from", "")
                to_label = arc.get("xlink:to", "")
                if from_label in loc_label_to_member:
                    from_members.add(loc_label_to_member[from_label])
                if to_label in loc_label_to_member:
                    to_members.add(loc_label_to_member[to_label])

            parent_member_names = from_members & to_members | (from_members - to_members)
            logs.append(f"def.xml: {len(loc_label_to_member)} locators, "
                        f"{len(from_members)} from-members, {len(to_members)} to-members, "
                        f"{len(parent_member_names)} parent members identified.")

            filtered = {}
            for seg_id, seg in unique_segments.items():
                member_key = seg["full_text"].replace(":", "_")
                if member_key in parent_member_names:
                    logs.append(f"  Excluded aggregate member: {seg_id} ({member_key})")
                else:
                    filtered[seg_id] = seg

            logs.append(
                f"Leaf-node filtering: {len(unique_segments)} -> {len(filtered)} segments "
                f"({len(unique_segments) - len(filtered)} aggregate members excluded)."
            )
            unique_segments = filtered
        except Exception as e:
            logs.append(f"Warning: Failed to parse def.xml for leaf-node filtering: {e}")
    else:
        logs.append("No def.xml available; skipping leaf-node filtering.")

    segment_details = []
    for seg_id, seg in unique_segments.items():
        ctx_dur = seg["context_id_dur"]
        ctx_inst = seg["context_id_inst"]
        segment_id = seg["segment_id"]

        logs.append(f"\n--- Checking Segment: {segment_id} ---")

        # Segment Name from label linkbase
        segment_name = segment_id

        # Find arc
        link_from = seg["full_text"].replace(":", "_")
        arcs = soup_lab.find_all(
            "link:labelArc", attrs={"xlink:from": link_from}
        )
        if not arcs:
            arcs = soup_lab.find_all(
                "labelArc", attrs={"xlink:from": link_from}
            )

        # Fallback to just the segment_id
        if not arcs:
            arcs = soup_lab.find_all(
                "link:labelArc", attrs={"xlink:from": segment_id}
            )
        if not arcs:
            arcs = soup_lab.find_all(
                "labelArc", attrs={"xlink:from": segment_id}
            )

        label_text = None
        for arc in arcs:
            label_id = arc.get("xlink:to")
            if label_id:
                # Prioritize standard label
                lbl = soup_lab.find(
                    attrs={
                        "xlink:label": label_id,
                        "xlink:role": "http://www.xbrl.org/2003/role/label",
                    }
                )
                if lbl:
                    label_text = lbl.text.strip()
                    break

        # Fallback to any label if standard label not found
        if not label_text and arcs:
            label_id = arcs[0].get("xlink:to")
            lbl = soup_lab.find(attrs={"xlink:label": label_id})
            if lbl:
                label_text = lbl.text.strip()

        if label_text:
            segment_name = label_text

        def find_val(tags, ctx, is_name_attr=True):
            for tg in tags:
                for t in facts_by_tag.get(tg, []):
                    if t.get("contextRef") == ctx:
                        return parse_num(t.text.strip())
            return None

        # Look up each segment metric defined in taxonomy
        seg_values: dict[str, object] = {}
        for metric_label, metric_def in SEGMENT_TAGS.items():
            ctx = ctx_dur if metric_def["context"] == "duration" else ctx_inst
            seg_values[metric_label] = find_val(metric_def["tags"], ctx)

        if any(v is not None for v in seg_values.values()):
            logs.append(f"  -> FOUND Data for {segment_name}")
            segment_details.append(
                {
                    "segment_name": segment_name,
                    "segment_revenue": seg_values.get("Sales to External Customers (外部顧客への売上高)"),
                    "segment_profit": seg_values.get("Segment Profit (セグメント利益)"),
                    "segment_employees": seg_values.get("Employees (連結従業員数)"),
                }
            )
        else:
            logs.append("  -> NO Data found for this context.")

    return company_summary, segment_details, logs


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
    q: str = Query(..., description="検索クエリ（企業名日英、銘柄コード、証券コード）"),
):
    """Search companies in BigQuery company_master."""
    from google.cloud import bigquery

    client = get_bq_client()
    normalized_q = unicodedata.normalize("NFKC", q.strip())

    # Search by ticker_symbol, edinet_code, or company name (ja/en)
    query = f"""
    SELECT
        edinet_code, ticker_symbol, company_name_ja, company_name_en,
        industry, jcn, latest_period_end
    FROM `{BQ_DATASET}.{BQ_COMPANY_MASTER}`
    WHERE
        ticker_symbol = @q
        OR edinet_code = @q
        OR LOWER(company_name_ja) LIKE LOWER(@q_like)
        OR LOWER(company_name_en) LIKE LOWER(@q_like)
        OR company_name_ja LIKE @q_like
    ORDER BY company_name_ja
    LIMIT 50
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("q", "STRING", normalized_q),
            bigquery.ScalarQueryParameter("q_like", "STRING", f"%{normalized_q}%"),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    results = []
    for _, row in df.iterrows():
        results.append(
            {
                "edinet_code": row["edinet_code"],
                "ticker_symbol": row.get("ticker_symbol"),
                "company_name_ja": row.get("company_name_ja"),
                "company_name_en": row.get("company_name_en"),
                "industry": row.get("industry"),
                "jcn": row.get("jcn"),
                "latest_period_end": str(row["latest_period_end"]) if row.get("latest_period_end") else None,
            }
        )
    return {"results": results}


@app.get("/api/companies/{edinet_code}")
def api_company_detail(edinet_code: str):
    """Get company detail: master info + all financial summaries + segment data."""
    from google.cloud import bigquery
    import pandas as pd

    client = get_bq_client()

    # 1. Company master info
    master_query = f"""
    SELECT *
    FROM `{BQ_DATASET}.{BQ_COMPANY_MASTER}`
    WHERE edinet_code = @code
    LIMIT 1
    """
    master_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", edinet_code),
        ]
    )
    df_master = client.query(master_query, job_config=master_config).to_dataframe()

    if df_master.empty:
        raise HTTPException(status_code=404, detail=f"Company not found: {edinet_code}")

    master = df_master.iloc[0]
    company_info = {
        "edinet_code": master["edinet_code"],
        "ticker_symbol": master.get("ticker_symbol"),
        "company_name_ja": master.get("company_name_ja"),
        "company_name_en": master.get("company_name_en"),
        "industry": master.get("industry"),
        "jcn": master.get("jcn"),
        "latest_period_end": str(master["latest_period_end"]) if pd.notnull(master.get("latest_period_end")) else None,
    }

    # 2. Financial summaries (time series)
    fin_query = f"""
    SELECT *
    FROM `{BQ_DATASET}.{BQ_FINANCIAL_SUMMARY}`
    WHERE edinet_code = @code
    ORDER BY period_end DESC
    """
    fin_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", edinet_code),
        ]
    )
    df_fin = client.query(fin_query, job_config=fin_config).to_dataframe()

    financials = []
    for _, row in df_fin.iterrows():
        financials.append(
            {
                "fiscal_year": int(row["fiscal_year"]) if pd.notnull(row.get("fiscal_year")) else None,
                "period_start": str(row["period_start"]) if pd.notnull(row.get("period_start")) else None,
                "period_end": str(row["period_end"]) if pd.notnull(row.get("period_end")) else None,
                "doc_id": row.get("doc_id"),
                "submit_date": str(row["submit_date"]) if pd.notnull(row.get("submit_date")) else None,
                "accounting_standard": row.get("accounting_standard"),
                "revenue": int(row["revenue"]) if pd.notnull(row.get("revenue")) else None,
                "net_income_loss": int(row["net_income_loss"]) if pd.notnull(row.get("net_income_loss")) else None,
                "total_assets": int(row["total_assets"]) if pd.notnull(row.get("total_assets")) else None,
                "net_assets": int(row["net_assets"]) if pd.notnull(row.get("net_assets")) else None,
                "roe": float(row["roe"]) if pd.notnull(row.get("roe")) else None,
                "pbr": float(row["pbr"]) if pd.notnull(row.get("pbr")) else None,
                "per": float(row["per"]) if pd.notnull(row.get("per")) else None,
            }
        )

    # 3. Segment data (grouped by fiscal year)
    seg_query = f"""
    SELECT *
    FROM `{BQ_DATASET}.{BQ_SEGMENT_DATA}`
    WHERE edinet_code = @code
    ORDER BY fiscal_year DESC, segment_name
    """
    seg_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", edinet_code),
        ]
    )
    df_seg = client.query(seg_query, job_config=seg_config).to_dataframe()

    segments: dict[int, list] = {}
    for _, row in df_seg.iterrows():
        fy = int(row["fiscal_year"]) if pd.notnull(row.get("fiscal_year")) else 0
        if fy not in segments:
            segments[fy] = []
        segments[fy].append(
            {
                "segment_name": row.get("segment_name"),
                "segment_revenue": int(row["segment_revenue"]) if pd.notnull(row.get("segment_revenue")) else None,
                "segment_profit": int(row["segment_profit"]) if pd.notnull(row.get("segment_profit")) else None,
                "segment_employees": int(row["segment_employees"]) if pd.notnull(row.get("segment_employees")) else None,
            }
        )

    return {
        "company": company_info,
        "financials": financials,
        "segments": segments,
    }


@app.post("/api/batch/process")
def api_batch_process(req: BatchProcessRequest, request: Request):
    """
    Batch process: find unprocessed documents in BigQuery,
    download XBRL, parse, and insert into financial_summary + segment_data.
    Also upsert company_master.
    """
    from google.cloud import bigquery
    import pandas as pd

    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    api_key = req.api_key or EDINET_API_KEY
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail="EDINET API Key is required. Set EDINET_API_KEY in .env or pass api_key.",
        )

    client = get_bq_client()
    project_id = _get_project_id()

    # 1. Find unprocessed documents
    unprocessed_query = f"""
    SELECT doc_id, edinet_code, sec_code, filer_name, jcn,
           period_start, period_end, submit_date_time
    FROM `{BQ_DATASET}.{BQ_DOCUMENTS}`
    WHERE (is_processed IS NULL OR is_processed = FALSE)
      AND edinet_code IS NOT NULL
      AND edinet_code != ''
    ORDER BY submit_date_time DESC
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INTEGER", req.limit),
        ]
    )
    df_docs = client.query(unprocessed_query, job_config=job_config).to_dataframe()

    if df_docs.empty:
        return {"processed": 0, "message": "No unprocessed documents found."}

    processed_count = 0
    errors = []

    for _, doc_row in df_docs.iterrows():
        doc_id = doc_row["doc_id"]
        edinet_code = doc_row["edinet_code"]

        try:
            # 2. Download and parse XBRL
            xbrl_content, lab_content, def_content = download_and_extract_xbrl(doc_id, api_key)

            if not xbrl_content or not lab_content:
                # Mark as processed even if no XBRL (to avoid retrying)
                _mark_document_processed(client, project_id, doc_id)
                processed_count += 1
                continue

            company_summary, segment_details, logs = extract_xbrl_data(
                xbrl_content, lab_content, def_content
            )

            # 3. Determine fiscal year from period_end
            period_end = doc_row.get("period_end")
            fiscal_year = None
            if pd.notnull(period_end):
                fiscal_year = int(str(period_end)[:4])

            # 4. Insert into financial_summary
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
            _insert_rows_load_job(client, project_id, BQ_FINANCIAL_SUMMARY, [fin_row])

            # 5. Insert into segment_data
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
                _insert_rows_load_job(client, project_id, BQ_SEGMENT_DATA, seg_rows)

            # 6. Upsert company_master
            sec_code = doc_row.get("sec_code")
            ticker_symbol = None
            if sec_code and len(str(sec_code)) >= 4:
                ticker_symbol = str(sec_code)[:4]

            _upsert_company_master(
                client, project_id, edinet_code,
                company_name_ja=doc_row.get("filer_name"),
                jcn=doc_row.get("jcn"),
                latest_period_end=str(period_end) if pd.notnull(period_end) else None,
            )

            # 7. Mark as processed
            _mark_document_processed(client, project_id, doc_id)
            processed_count += 1

            # Rate limit EDINET API calls
            time.sleep(1)

        except HTTPException:
            errors.append({"doc_id": doc_id, "error": "XBRL download failed"})
        except Exception as e:
            errors.append({"doc_id": doc_id, "error": str(e)})

    return {
        "processed": processed_count,
        "errors": errors,
        "total_found": len(df_docs),
    }


@app.post("/api/import/edinet-codelist")
async def api_import_edinet_codelist(request: Request):
    """
    Import EDINET code list CSV to populate company_master with
    English names, ticker symbols, and industry.

    The CSV should be the EDINET code list format with columns:
    EDINETコード, 提出者種別, 上場区分, 連結の有無, 資本金, 決算日,
    提出者名, 提出者名（英字）, 提出者名（ヨミ）, 所在地, 提出者業種,
    証券コード, 提出者法人番号
    """
    from google.cloud import bigquery

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
    # EDINET CSV has a metadata header on the first row ("ダウンロード日：...")
    if lines and "ダウンロード" in lines[0]:
        lines = lines[1:]

    reader = csv.DictReader(lines)

    client = get_bq_client()
    project_id = _get_project_id()
    rows = []

    for csv_row in reader:
        edinet_code = csv_row.get("ＥＤＩＮＥＴコード", "").strip()
        if not edinet_code:
            continue

        # 証券コード (5 digits) → 4 digits
        sec_code_raw = csv_row.get("証券コード", "").strip()
        ticker_symbol = None
        if sec_code_raw and len(sec_code_raw) >= 4:
            ticker_symbol = sec_code_raw[:4]

        company_name_ja = csv_row.get("提出者名", "").strip() or None
        company_name_en = csv_row.get("提出者名（英字）", "").strip() or None
        industry = csv_row.get("提出者業種", "").strip() or None
        jcn = csv_row.get("提出者法人番号", "").strip() or None

        rows.append({
            "edinet_code": edinet_code,
            "ticker_symbol": ticker_symbol,
            "company_name_ja": company_name_ja,
            "company_name_en": company_name_en,
            "industry": industry,
            "jcn": jcn,
            "last_modified": None,
            "latest_period_end": None,
        })

    if not rows:
        return {"imported": 0, "message": "No valid rows found in CSV."}

    import json
    
    # 既存のデータを全削除するのではなく、MERGE文で更新（UPSERT）する
    # ticker_symbol または company_name_en が異なる（または空の）場合のみ更新する
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
        ticker_symbol = S.ticker_symbol,
        company_name_en = S.company_name_en,
        industry = S.industry,
        last_modified = CURRENT_TIMESTAMP()
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("json_str", "STRING", json_data),
            ]
        )
        client.query(query, job_config=job_config).result()
    except Exception as e:
        return {"error": str(e), "message": "Failed to execute MERGE query"}

    return {"imported": len(rows), "message": "Merged successfully"}


# ---------------------------------------------------------------------------
# Helper Functions for BigQuery writes
# ---------------------------------------------------------------------------


def _insert_rows_load_job(client, project_id: str, table_id: str, rows: list[dict]):
    """Insert rows into BigQuery using a Load Job (sandbox-compatible)."""
    import json
    from google.cloud import bigquery

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
    load_job.result()  # Wait for completion


def _mark_document_processed(client, project_id: str, doc_id: str):
    """Mark a document as processed in the documents table."""
    from google.cloud import bigquery

    query = f"""
    UPDATE `{project_id}.{BQ_DATASET}.{BQ_DOCUMENTS}`
    SET is_processed = TRUE
    WHERE doc_id = @doc_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("doc_id", "STRING", doc_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def _upsert_company_master(client, project_id: str, edinet_code: str, **kwargs):
    """Upsert a row in company_master using MERGE."""
    from google.cloud import bigquery

    # Build SET clause dynamically from non-None kwargs
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


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
