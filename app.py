import streamlit as st
import requests
import zipfile
import io
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Page Configuration ---
st.set_page_config(
    page_title="EDINET Segment Sales Viewer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Custom Styling ---
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    .stApp { background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); }
    h1 { color: #1e3a8a; font-family: 'Inter', sans-serif; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    # 1. Try Streamlit Secrets (Streamlit Cloud)
    try:
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception:
        pass
        
    # 2. Try Local credentials.json
    if os.path.exists("credentials.json"):
        credentials = service_account.Credentials.from_service_account_file("credentials.json")
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
        
    # 3. Fallback to default (e.g. GOOGLE_APPLICATION_CREDENTIALS env var)
    return bigquery.Client()

def search_documents(filer_name, period_end):
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
        params.append(bigquery.ScalarQueryParameter("filer_name", "STRING", f"%{filer_name}%"))
    if period_end:
        query += " AND period_end = @period_end"
        params.append(bigquery.ScalarQueryParameter("period_end", "DATE", period_end))
    
    query += " ORDER BY submit_date_time DESC LIMIT 50"
    
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return df

def get_api_key():
    # 1. Try environment variable
    api_key = os.environ.get("EDINET_API_KEY")
    if api_key: return api_key
    
    # 2. Try Streamlit secrets
    try:
        if "EDINET_API_KEY" in st.secrets:
            return st.secrets["EDINET_API_KEY"]
    except Exception:
        pass
    return None

def download_and_extract_xbrl(doc_id, api_key):
    if not api_key:
        st.error("EDINET API Key is missing.")
        return None, None
    
    url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}?type=1&Subscription-Key={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        st.error(f"Failed to download document from EDINET API (Status {resp.status_code})")
        return None, None
    
    try:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        st.error("Downloaded file from EDINET is not a valid zip file. The document might not have XBRL attached.")
        return None, None
    
    xbrl_content = None
    lab_content = None
    
    # In EDINET, the .xbrl file contains all the facts in one single file, 
    # whereas .htm files are split into multiple files. We should prioritize .xbrl.
    xbrl_filename = next((f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith(".xbrl")), None)
    if xbrl_filename:
        xbrl_content = z.read(xbrl_filename)
    else:
        # Fallback to the first .htm if no .xbrl is found (rare)
        htm_filename = next((f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith(".htm")), None)
        if htm_filename:
            xbrl_content = z.read(htm_filename)
            
    lab_filename = next((f for f in z.namelist() if f.startswith("XBRL/PublicDoc/") and f.endswith("_lab.xml")), None)
    if lab_filename:
        lab_content = z.read(lab_filename)
            
    return xbrl_content, lab_content

def parse_num(val):
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val

def extract_xbrl_data(xbrl_content, lab_content):
    from bs4 import BeautifulSoup
    logs = []
    if not xbrl_content or not lab_content:
        logs.append("Error: Missing XBRL or Label Linkbase content.")
        return {}, [], logs
        
    try:
        soup_xbrl = BeautifulSoup(xbrl_content, 'xml')
        soup_lab = BeautifulSoup(lab_content, 'xml')
        logs.append("Successfully parsed XBRL and Label Linkbase XML.")
    except Exception as e:
        st.error(f"Error parsing XML: {e}")
        logs.append(f"Error parsing XML: {e}")
        return {}, [], logs
    
    # --- 1. Extract Company Summary ---
    logs.append("\n--- Extracting Company Summary ---")
    def get_first_text(metric_name, tags):
        valid_contexts = ['CurrentYearDuration', 'CurrentYearInstant', 'FilingDateInstant']
        for tag_name in tags:
            # EDINET namespaces vary, so search by suffix
            for t in soup_xbrl.find_all(True):
                t_name = t.name or ""
                if t_name.endswith(tag_name):
                    ctx = t.get("contextRef", "")
                    if ctx in valid_contexts:
                        val = parse_num(t.text.strip())
                        logs.append(f"  -> FOUND {metric_name} ({t_name}): {val} [Context: {ctx}]")
                        return val
        logs.append(f"  -> NO Data found for {metric_name}. Tried tags: {', '.join(tags)}")
        return None
        
    # Get Period Start from CurrentYearDuration context
    period_start = None
    cy_ctx = soup_xbrl.find("context", id="CurrentYearDuration")
    if cy_ctx:
        period_node = cy_ctx.find("period")
        if period_node and period_node.find("startDate"):
            period_start = period_node.find("startDate").text.strip()
            
    per_val = get_first_text('PER', ['PriceEarningsRatioIFRSSummaryOfBusinessResults', 'PriceEarningsRatioSummaryOfBusinessResults'])
    roe_val = get_first_text('ROE', ['RateOfReturnOnEquityIFRSSummaryOfBusinessResults', 'RateOfReturnOnEquitySummaryOfBusinessResults'])
    pbr_val = None
    if isinstance(per_val, (int, float)) and isinstance(roe_val, (int, float)):
        pbr_val = per_val * roe_val
        
    company_summary = {
        'Period Start (期首)': period_start,
        'Accounting Standards (会計基準)': get_first_text('Accounting Standards', ['AccountingStandardsDEI']),
        'Net Sales (売上高)': get_first_text('Net Sales', ['NetSalesIFRS', 'NetSales', 'RevenueIFRS']),
        'Net Income (純利益)': get_first_text('Net Income', ['ProfitLossIFRS', 'ProfitLossAttributableToOwnersOfParent', 'ProfitLossAttributableToOwnersOfParentIFRS']),
        'Total Assets (総資産)': get_first_text('Total Assets', ['AssetsIFRS', 'Assets']),
        'PER (株価収益率)': per_val,
        'PBR (株価純資産倍率)': pbr_val,
        'ROE (自己資本利益率)': roe_val,
    }
    
    # --- 2. Extract Segment Details ---
    explicit_members = soup_xbrl.find_all(attrs={"dimension": "jpcrp_cor:OperatingSegmentsAxis"})
    logs.append(f"Found {len(explicit_members)} elements with dimension='jpcrp_cor:OperatingSegmentsAxis'.")
    
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
            "context_id_inst": "CurrentYearInstant_" + text.replace(":", "")
        }
    
    logs.append(f"Extracted {len(unique_segments)} unique segments.")
        
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
        arcs = soup_lab.find_all("link:labelArc", attrs={"xlink:from": link_from})
        if not arcs:
            arcs = soup_lab.find_all("labelArc", attrs={"xlink:from": link_from})
            
        # Fallback to just the segment_id
        if not arcs:
            arcs = soup_lab.find_all("link:labelArc", attrs={"xlink:from": segment_id})
        if not arcs:
            arcs = soup_lab.find_all("labelArc", attrs={"xlink:from": segment_id})
            
        label_text = None
        for arc in arcs:
            label_id = arc.get("xlink:to")
            if label_id:
                # Prioritize standard label
                lbl = soup_lab.find(attrs={"xlink:label": label_id, "xlink:role": "http://www.xbrl.org/2003/role/label"})
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
                name_attr = t.get("name") or ""
                t_name = t.name or ""
                if any(t_name.endswith(tg) or (is_name_attr and name_attr.endswith(tg)) for tg in tags):
                    return parse_num(t.text.strip())
            return None
            
        sales_val = find_val(['RevenuesFromExternalCustomers', 'SalesToExternalCustomersIFRS', 'RevenueFromExternalCustomersIFRS', 'NetSales'], ctx_dur)
        profit_val = find_val(['SegmentProfitLossIFRS', 'SegmentProfitLoss', 'ProfitLossAttributableToOwnersOfParent', 'ProfitLossBeforeTaxIFRS'], ctx_dur)
        emp_val = find_val(['NumberOfEmployees'], ctx_inst)
        
        if sales_val is not None or profit_val is not None or emp_val is not None:
            logs.append(f"  -> FOUND Data for {segment_name}")
            segment_details.append({
                "Segment ID": segment_id,
                "Segment Name": segment_name,
                "Sales to External Customers (外部顧客への売上高)": sales_val,
                "Segment Profit (セグメント利益)": profit_val,
                "Employees (連結従業員数)": emp_val
            })
        else:
            logs.append("  -> NO Data found for this context.")
        
    return company_summary, segment_details, logs

# --- Main App Interface ---
st.title("📊 EDINET XBRL Viewer (Company & Segments)")
st.markdown("Search for filings in BigQuery, download their XBRL data via EDINET API, and extract company summary and segment details.")

st.sidebar.title("🔑 Authentication")
current_api_key = get_api_key()
if not current_api_key:
    st.sidebar.warning("環境変数 `EDINET_API_KEY` が設定されていません。")
    user_api_key = st.sidebar.text_input("EDINET API Key (一時入力用)")
    active_api_key = user_api_key
else:
    active_api_key = current_api_key
    st.sidebar.success("API Key loaded successfully.")

st.sidebar.title("🔍 Search Options")
filer_name = st.sidebar.text_input("企業名 (Filer Name)", "")
period_end_str = st.sidebar.text_input("決算日 (Period End) YYYY-MM-DD", "")

if st.sidebar.button("Search Documents"):
    if not filer_name and not period_end_str:
        st.sidebar.warning("Please provide either Filer Name or Period End to search.")
    else:
        with st.spinner("Searching BigQuery..."):
            try:
                df = search_documents(filer_name, period_end_str)
                st.session_state["search_results"] = df
            except Exception as e:
                st.error(f"BigQuery Error: {e}")

if "search_results" in st.session_state:
    df = st.session_state["search_results"]
    if df.empty:
        st.info("No documents found matching the search criteria.")
    else:
        st.success(f"Found {len(df)} document(s).")
        
        options = df.apply(lambda row: f"{row['filer_name']} ({row['period_end']}) - {row['doc_description']} [{row['doc_id']}]", axis=1).tolist()
        selected_option = st.selectbox("Select a Document to Analyze", options)
        
        if st.button("Download & Analyze XBRL"):
            if not active_api_key:
                st.error("Please provide an EDINET API Key in the sidebar or set the EDINET_API_KEY environment variable.")
            else:
                selected_index = options.index(selected_option)
                selected_row = df.iloc[selected_index]
                selected_doc_id = selected_row['doc_id']
                
                with st.spinner(f"Downloading and analyzing document {selected_doc_id} from EDINET..."):
                    xbrl_content, lab_content = download_and_extract_xbrl(selected_doc_id, active_api_key)
                if xbrl_content and lab_content:
                    company_summary, segment_details, logs = extract_xbrl_data(xbrl_content, lab_content)
                    
                    with st.expander("🔍 Extraction Debug Logs (Click to expand)"):
                        st.code("\n".join(logs), language="text")
                        
                    # 1. Company Summary Display
                    st.header("🏢 Company Summary")
                    
                    period_start_val = company_summary.pop('Period Start (期首)', 'N/A')
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Doc ID**: `{selected_doc_id}`")
                        st.markdown(f"**Filer Name**: {selected_row['filer_name']}")
                        st.markdown(f"**Period Start**: {period_start_val}")
                        st.markdown(f"**Period End**: {selected_row['period_end']}")
                    with col2:
                        html_url = f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?{selected_doc_id}"
                        pdf_url = f"https://disclosure2dl.edinet-fsa.go.jp/searchdocument/pdf/{selected_doc_id}.pdf"
                        st.markdown(f"**Original Document**: [HTML]({html_url}) | [PDF]({pdf_url})")
                        
                        api_xbrl_url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{selected_doc_id}?type=1&Subscription-Key={active_api_key}"
                        api_csv_url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{selected_doc_id}?type=5&Subscription-Key={active_api_key}"
                        st.markdown(f"**Download API**: [XBRL (type=1)]({api_xbrl_url}) | [CSV (type=5)]({api_csv_url})")
                    
                    st.subheader("Financial Highlights")
                    metrics_cols = st.columns(3)
                    metrics_keys = list(company_summary.keys())
                    for i, key in enumerate(metrics_keys):
                        val = company_summary[key]
                        if val is None or val == "":
                            display_val = "N/A"
                        elif "ROE" in key and isinstance(val, (int, float)):
                            display_val = f"{val * 100:.2f}%"
                        elif ("PER" in key or "PBR" in key) and isinstance(val, (int, float)):
                            display_val = f"{val:.2f}"
                        elif isinstance(val, (int, float)):
                            display_val = f"{val:,.0f}"
                        else:
                            display_val = str(val)
                        metrics_cols[i % 3].metric(label=key, value=display_val)
                    st.divider()
                        
                    # 2. Segment Details Display
                    st.header("📊 Segment Details")
                    if segment_details:
                        import pandas as pd
                        import plotly.express as px
                        
                        res_df = pd.DataFrame(segment_details)
                        st.dataframe(res_df, use_container_width=True)
                        st.info("Note: Values are shown exactly as extracted from the XBRL. Empty values mean the tag was not found for that segment context.")
                        
                        st.subheader("Segment Breakdown")
                        pie_cols = st.columns(3)
                        
                        # Sales Pie Chart
                        with pie_cols[0]:
                            sales_col = "Sales to External Customers (外部顧客への売上高)"
                            if sales_col in res_df.columns:
                                # Convert to numeric just in case, coerce errors to NaN
                                res_df[sales_col] = pd.to_numeric(res_df[sales_col], errors='coerce')
                                sales_df = res_df[res_df[sales_col].notna() & (res_df[sales_col] > 0)]
                                if not sales_df.empty:
                                    fig_sales = px.pie(sales_df, values=sales_col, names="Segment Name", title="Sales")
                                    st.plotly_chart(fig_sales, use_container_width=True)
                                else:
                                    st.write("No positive sales data for chart.")
                                    
                        # Profit Pie Chart
                        with pie_cols[1]:
                            profit_col = "Segment Profit (セグメント利益)"
                            if profit_col in res_df.columns:
                                res_df[profit_col] = pd.to_numeric(res_df[profit_col], errors='coerce')
                                profit_df = res_df[res_df[profit_col].notna() & (res_df[profit_col] > 0)]
                                if not profit_df.empty:
                                    fig_profit = px.pie(profit_df, values=profit_col, names="Segment Name", title="Profit")
                                    st.plotly_chart(fig_profit, use_container_width=True)
                                else:
                                    st.write("No positive profit data for chart.")
                                    
                        # Employees Pie Chart
                        with pie_cols[2]:
                            emp_col = "Employees (連結従業員数)"
                            if emp_col in res_df.columns:
                                res_df[emp_col] = pd.to_numeric(res_df[emp_col], errors='coerce')
                                emp_df = res_df[res_df[emp_col].notna() & (res_df[emp_col] > 0)]
                                if not emp_df.empty:
                                    fig_emp = px.pie(emp_df, values=emp_col, names="Segment Name", title="Employees")
                                    st.plotly_chart(fig_emp, use_container_width=True)
                                else:
                                    st.write("No positive employee data for chart.")
                    else:
                        st.warning("No segment details found in this document. (This company may not report segment sales/profits or the data format differs from expected).")
                elif xbrl_content is None and lab_content is None:
                    # Error already shown in download function
                    pass
                else:
                    st.warning("Could not locate both the necessary main XBRL HTML file and the Label Linkbase XML file in the downloaded package.")

st.markdown("---")
st.caption("Powered by Streamlit | Data from EDINET API & BigQuery")
