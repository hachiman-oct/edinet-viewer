"""
EDINET XBRL Downloader & Parser
===============================

EDINET API からの XBRL ZIP ダウンロード（指数バックオフ・リトライ付き）と、
XBRL / Label / Definition Linkbase XML のパース処理を提供する共通モジュール。
"""

import io
import logging
import re
import time
import zipfile
from collections import defaultdict
from typing import Any

import requests as http_requests
from bs4 import BeautifulSoup

from taxonomy import COMPANY_TAGS, SEGMENT_TAGS

logger = logging.getLogger(__name__)

EDINET_API_DOC_URL = (
    "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    "?type=1&Subscription-Key={api_key}"
)

# リトライ設定
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5  # 秒（指数バックオフ: 5, 10, 20）
RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

# TextBlock 除去用正規表現（プリコンパイルによる高速化）
_RE_TEXTBLOCK_PAIR = re.compile(
    r"<([a-zA-Z0-9_:]*TextBlock)\b[^>]*>.*?</\1>", flags=re.DOTALL
)
_RE_TEXTBLOCK_SELF = re.compile(r"<([a-zA-Z0-9_:]*TextBlock)\b[^>]*/>")


def extract_files_from_zip_bytes(
    raw_bytes: bytes,
) -> tuple[bytes | None, bytes | None, bytes | None]:
    """ZIP バイト列から (.xbrl/.htm, _lab.xml, _def.xml) を抽出する。"""
    z = zipfile.ZipFile(io.BytesIO(raw_bytes))
    names = z.namelist()

    xbrl_content = None
    lab_content = None
    def_content = None

    # .xbrl を優先し、なければ .htm にフォールバック
    xbrl_file = next(
        (
            f
            for f in names
            if f.startswith("XBRL/PublicDoc/") and f.endswith(".xbrl")
        ),
        None,
    )
    if xbrl_file:
        xbrl_content = z.read(xbrl_file)
    else:
        htm_file = next(
            (
                f
                for f in names
                if f.startswith("XBRL/PublicDoc/") and f.endswith(".htm")
            ),
            None,
        )
        if htm_file:
            xbrl_content = z.read(htm_file)

    lab_file = next(
        (
            f
            for f in names
            if f.startswith("XBRL/PublicDoc/") and f.endswith("_lab.xml")
        ),
        None,
    )
    if lab_file:
        lab_content = z.read(lab_file)

    def_file = next(
        (
            f
            for f in names
            if f.startswith("XBRL/PublicDoc/") and f.endswith("_def.xml")
        ),
        None,
    )
    if def_file:
        def_content = z.read(def_file)

    return xbrl_content, lab_content, def_content


def download_xbrl_with_retry(
    doc_id: str, api_key: str
) -> tuple[bytes | None, bytes | None, bytes | None]:
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
                    resp.status_code,
                    doc_id,
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logger.error(
                    "  EDINET API returned %d for %s (non-retryable). Skipping.",
                    resp.status_code,
                    doc_id,
                )
                return None, None, None

            try:
                return extract_files_from_zip_bytes(resp.content)
            except zipfile.BadZipFile:
                logger.warning(
                    "  Downloaded file for %s is not a valid ZIP. Skipping.",
                    doc_id,
                )
                return None, None, None

        except http_requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning(
                "  Network error for %s (attempt %d/%d): %s. Retrying in %ds...",
                doc_id,
                attempt,
                MAX_RETRIES,
                str(e),
                wait,
            )
            time.sleep(wait)

    logger.error(
        "  All %d retries exhausted for %s. Skipping.", MAX_RETRIES, doc_id
    )
    return None, None, None


def parse_num(val: Any):
    """文字列を int または float に変換する（変換不可の場合はそのまま返す）。"""
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def extract_xbrl_data(
    xbrl_content: bytes | str | None,
    lab_content: bytes | str | None,
    def_content: bytes | str | None = None,
) -> tuple[dict, list[dict], list[str]]:
    """
    XBRL / Label / Definition XML から財務サマリーとセグメント情報を抽出する。
    def.xml を用いて親（集計）セグメントを除外し、末端セグメントのみを残す。
    """
    logs: list[str] = []
    if not xbrl_content or not lab_content:
        logs.append("Error: Missing XBRL or Label Linkbase content.")
        return {}, [], logs

    try:
        xbrl_str = (
            xbrl_content.decode("utf-8")
            if isinstance(xbrl_content, bytes)
            else xbrl_content
        )
        # TextBlock 要素を事前除去してパース速度・メモリ効率を向上
        xbrl_str = _RE_TEXTBLOCK_PAIR.sub("", xbrl_str)
        xbrl_str = _RE_TEXTBLOCK_SELF.sub("", xbrl_str)

        soup_xbrl = BeautifulSoup(xbrl_str, "xml")
        soup_lab = BeautifulSoup(lab_content, "xml")
        logs.append("Successfully parsed XBRL and Label Linkbase XML.")
    except Exception as e:
        logs.append(f"Error parsing XML: {e}")
        return {}, [], logs

    # タグ名で O(1) ルックアップできるようインデックスを構築
    facts_by_tag: dict[str, list] = defaultdict(list)
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
    valid_contexts = {
        "CurrentYearDuration",
        "CurrentYearInstant",
        "FilingDateInstant",
    }

    def get_first_text(metric_name: str, tags: list[str]):
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
        "revenue": get_first_text("Net Sales", COMPANY_TAGS["Net Sales"]),
        "net_income_loss": get_first_text(
            "Net Income", COMPANY_TAGS["Net Income"]
        ),
        "total_assets": get_first_text(
            "Total Assets", COMPANY_TAGS["Total Assets"]
        ),
        "net_assets": get_first_text("Net Assets", COMPANY_TAGS["Net Assets"]),
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

    unique_segments: dict[str, dict] = {}
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

    logs.append(
        f"Extracted {len(unique_segments)} unique segments (before leaf-node filtering)."
    )

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
            for arc in soup_def.find_all(
                ["link:definitionArc", "definitionArc"]
            ):
                from_label = arc.get("xlink:from", "")
                to_label = arc.get("xlink:to", "")
                if from_label in loc_label_to_member:
                    from_members.add(loc_label_to_member[from_label])
                if to_label in loc_label_to_member:
                    to_members.add(loc_label_to_member[to_label])

            parent_member_names = from_members
            logs.append(
                f"def.xml: {len(loc_label_to_member)} locators, "
                f"{len(from_members)} from-members, {len(to_members)} to-members, "
                f"{len(parent_member_names)} parent members identified."
            )

            filtered = {}
            for seg_id, seg in unique_segments.items():
                member_key = seg["full_text"].replace(":", "_")
                if member_key in parent_member_names:
                    logs.append(
                        f"  Excluded aggregate member: {seg_id} ({member_key})"
                    )
                else:
                    filtered[seg_id] = seg

            logs.append(
                f"Leaf-node filtering: {len(unique_segments)} -> {len(filtered)} segments "
                f"({len(unique_segments) - len(filtered)} aggregate members excluded)."
            )
            unique_segments = filtered
        except Exception as e:
            logs.append(
                f"Warning: Failed to parse def.xml for leaf-node filtering: {e}"
            )
    else:
        logs.append("No def.xml available; skipping leaf-node filtering.")

    # Label Linkbase を事前インデックス化して O(1) でラベル解決
    arcs_by_from: dict[str, list[str]] = defaultdict(list)
    for arc in soup_lab.find_all(["link:labelArc", "labelArc"]):
        frm = arc.get("xlink:from")
        to = arc.get("xlink:to")
        if frm and to:
            arcs_by_from[frm].append(to)

    std_labels_by_id: dict[str, str] = {}
    any_labels_by_id: dict[str, str] = {}
    for lbl in soup_lab.find_all(["link:label", "label"]):
        lid = lbl.get("xlink:label")
        if not lid:
            continue
        txt = lbl.text.strip()
        if lid not in any_labels_by_id:
            any_labels_by_id[lid] = txt
        if lbl.get("xlink:role") == "http://www.xbrl.org/2003/role/label":
            std_labels_by_id[lid] = txt

    segment_details = []
    for seg_id, seg in unique_segments.items():
        ctx_dur = seg["context_id_dur"]
        ctx_inst = seg["context_id_inst"]
        segment_id = seg["segment_id"]

        logs.append(f"\n--- Checking Segment: {segment_id} ---")

        segment_name = segment_id
        link_from = seg["full_text"].replace(":", "_")
        target_ids = arcs_by_from.get(link_from) or arcs_by_from.get(
            segment_id, []
        )

        label_text = None
        for lid in target_ids:
            if lid in std_labels_by_id:
                label_text = std_labels_by_id[lid]
                break
        if not label_text and target_ids:
            label_text = any_labels_by_id.get(target_ids[0])

        if label_text:
            segment_name = label_text

        def find_val(tags: list[str], ctx: str):
            for tg in tags:
                for t in facts_by_tag.get(tg, []):
                    if t.get("contextRef") == ctx:
                        return parse_num(t.text.strip())
            return None

        seg_values: dict[str, Any] = {}
        for metric_label, metric_def in SEGMENT_TAGS.items():
            ctx = (
                ctx_dur if metric_def["context"] == "duration" else ctx_inst
            )
            seg_values[metric_label] = find_val(metric_def["tags"], ctx)

        if any(v is not None for v in seg_values.values()):
            logs.append(f"  -> FOUND Data for {segment_name}")
            segment_details.append(
                {
                    "segment_name": segment_name,
                    "segment_revenue": seg_values.get(
                        "Sales to External Customers (外部顧客への売上高)"
                    ),
                    "segment_profit": seg_values.get(
                        "Segment Profit (セグメント利益)"
                    ),
                    "segment_employees": seg_values.get(
                        "Employees (連結従業員数)"
                    ),
                }
            )
        else:
            logs.append("  -> NO Data found for this context.")

    return company_summary, segment_details, logs

