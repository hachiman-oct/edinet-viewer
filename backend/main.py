"""
EDINET XBRL Viewer — FastAPI Backend
Migrated from app.py (Streamlit) with logic preserved.
"""

import os
import io
import zipfile
from typing import Optional

import requests as http_requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from taxonomy import COMPANY_TAGS, SEGMENT_TAGS

# Load environment variables
load_dotenv()

app = FastAPI(
    title="EDINET XBRL Viewer API",
    description="Search EDINET filings via BigQuery, download XBRL, and extract company & segment data.",
    version="1.0.0",
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


# ---------------------------------------------------------------------------
# Data Functions (migrated from app.py — logic unchanged)
# ---------------------------------------------------------------------------


def search_documents(filer_name: Optional[str], period_end: Optional[str]):
    """Search EDINET documents in BigQuery."""
    from google.cloud import bigquery

    client = get_bq_client()
    query = """
    SELECT doc_id, filer_name, period_end, submit_date_time, doc_description
    FROM `edinet.documents`
    WHERE 1=1
    """
    params = []
    if filer_name:
        query += " AND filer_name_normalized LIKE @filer_name"
        params.append(
            bigquery.ScalarQueryParameter("filer_name", "STRING", f"%{filer_name}%")
        )
    if period_end:
        query += " AND period_end = @period_end"
        params.append(
            bigquery.ScalarQueryParameter("period_end", "DATE", period_end)
        )

    query += " ORDER BY submit_date_time DESC LIMIT 50"

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def download_and_extract_xbrl(doc_id: str, api_key: str):
    """Download ZIP from EDINET API and extract XBRL + label + definition content."""
    if not api_key:
        raise HTTPException(status_code=400, detail="EDINET API Key is missing.")

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
        soup_xbrl = BeautifulSoup(xbrl_content, "xml")
        soup_lab = BeautifulSoup(lab_content, "xml")
        logs.append("Successfully parsed XBRL and Label Linkbase XML.")
    except Exception as e:
        logs.append(f"Error parsing XML: {e}")
        return {}, [], logs

    # --- 1. Extract Company Summary ---
    logs.append("\n--- Extracting Company Summary ---")

    def get_first_text(metric_name, tags):
        valid_contexts = [
            "CurrentYearDuration",
            "CurrentYearInstant",
            "FilingDateInstant",
        ]
        for tag_name in tags:
            for t in soup_xbrl.find_all(True):
                t_name = t.name or ""
                local_name = t_name.rsplit(":", 1)[-1]
                if local_name == tag_name:
                    ctx = t.get("contextRef", "")
                    if ctx in valid_contexts:
                        val = parse_num(t.text.strip())
                        logs.append(
                            f"  -> FOUND {metric_name} ({t_name}): {val} [Context: {ctx}]"
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
        "Period Start (期首)": period_start,
        "Accounting Standards (会計基準)": get_first_text(
            "Accounting Standards", COMPANY_TAGS["Accounting Standards"]
        ),
        "Net Sales (売上高)": get_first_text(
            "Net Sales", COMPANY_TAGS["Net Sales"]
        ),
        "Net Income (純利益)": get_first_text(
            "Net Income", COMPANY_TAGS["Net Income"]
        ),
        "Total Assets (総資産)": get_first_text(
            "Total Assets", COMPANY_TAGS["Total Assets"]
        ),
        "PER (株価収益率)": per_val,
        "PBR (株価純資産倍率)": pbr_val,
        "ROE (自己資本利益率)": roe_val,
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

            # Build mapping: locator label → member name (from loc elements)
            # loc elements look like:
            #   <link:loc xlink:label="TotalMember" xlink:href="...#TotalMember" />
            # The member name is the fragment after '#' in href
            loc_label_to_member: dict[str, str] = {}
            for loc in soup_def.find_all(["link:loc", "loc"]):
                label = loc.get("xlink:label", "")
                href = loc.get("xlink:href", "")
                # Extract element name from href fragment (after '#')
                if "#" in href:
                    member_name = href.split("#")[-1]
                    loc_label_to_member[label] = member_name

            # Collect member names that appear as xlink:from in definitionArc
            # These are parent/aggregate nodes and should be excluded
            from_members: set[str] = set()
            to_members: set[str] = set()
            for arc in soup_def.find_all(["link:definitionArc", "definitionArc"]):
                from_label = arc.get("xlink:from", "")
                to_label = arc.get("xlink:to", "")
                if from_label in loc_label_to_member:
                    from_members.add(loc_label_to_member[from_label])
                if to_label in loc_label_to_member:
                    to_members.add(loc_label_to_member[to_label])

            # Leaf nodes = members that appear only in xlink:to, never in xlink:from
            # (i.e. they have no children in the hierarchy)
            parent_member_names = from_members & to_members | (from_members - to_members)
            logs.append(f"def.xml: {len(loc_label_to_member)} locators, "
                        f"{len(from_members)} from-members, {len(to_members)} to-members, "
                        f"{len(parent_member_names)} parent members identified.")

            # Filter: keep only segments whose full member key is NOT a parent
            # full_text "jpcrp...:TotalMember" → replace ":" with "_" to match
            # the href fragment format used in def.xml locators
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
            for t in soup_xbrl.find_all(True):
                if t.get("contextRef") != ctx:
                    continue
                name_attr = (t.get("name") or "").rsplit(":", 1)[-1]
                t_local = (t.name or "").rsplit(":", 1)[-1]
                if any(
                    t_local == tg or (is_name_attr and name_attr == tg)
                    for tg in tags
                ):
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
                    "Segment ID": segment_id,
                    "Segment Name": segment_name,
                    **seg_values,
                }
            )
        else:
            logs.append("  -> NO Data found for this context.")

    return company_summary, segment_details, logs


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class AnalyzeRequest(BaseModel):
    doc_id: str
    api_key: str


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/search")
def api_search(
    filer_name: Optional[str] = Query(None, description="企業名 (部分一致)"),
    period_end: Optional[str] = Query(None, description="決算日 (YYYY-MM-DD)"),
):
    """Search EDINET documents in BigQuery."""
    if not filer_name and not period_end:
        raise HTTPException(
            status_code=400,
            detail="Please provide either filer_name or period_end to search.",
        )
    try:
        df = search_documents(filer_name, period_end)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Error: {e}")

    # Convert DataFrame to list of dicts, handling date serialization
    results = []
    for _, row in df.iterrows():
        results.append(
            {
                "doc_id": row["doc_id"],
                "filer_name": row["filer_name"],
                "period_end": str(row["period_end"]) if row["period_end"] else None,
                "submit_date_time": (
                    str(row["submit_date_time"]) if row["submit_date_time"] else None
                ),
                "doc_description": row["doc_description"],
            }
        )
    return {"results": results}


@app.post("/api/analyze")
def api_analyze(req: AnalyzeRequest):
    """Download XBRL from EDINET and extract company summary + segment details."""
    xbrl_content, lab_content, def_content = download_and_extract_xbrl(req.doc_id, req.api_key)

    if xbrl_content is None and lab_content is None:
        raise HTTPException(
            status_code=422,
            detail="Could not extract XBRL content from the downloaded package.",
        )

    if not xbrl_content or not lab_content:
        raise HTTPException(
            status_code=422,
            detail="Could not locate both the main XBRL file and the Label Linkbase XML file in the downloaded package.",
        )

    company_summary, segment_details, logs = extract_xbrl_data(
        xbrl_content, lab_content, def_content
    )

    return {
        "company_summary": company_summary,
        "segment_details": segment_details,
        "logs": logs,
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
