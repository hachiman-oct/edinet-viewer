// TypeScript type definitions for EDINET XBRL Viewer

export interface SearchResult {
  doc_id: string;
  filer_name: string;
  period_end: string | null;
  submit_date_time: string | null;
  doc_description: string;
}

export interface SearchResponse {
  results: SearchResult[];
}

export interface CompanySummary {
  "Period Start (期首)": string | null;
  "Accounting Standards (会計基準)": string | number | null;
  "Net Sales (売上高)": number | null;
  "Net Income (純利益)": number | null;
  "Total Assets (総資産)": number | null;
  "PER (株価収益率)": number | null;
  "PBR (株価純資産倍率)": number | null;
  "ROE (自己資本利益率)": number | null;
  [key: string]: string | number | null;
}

export interface SegmentDetail {
  "Segment ID": string;
  "Segment Name": string;
  "Sales to External Customers (外部顧客への売上高)": number | null;
  "Segment Profit (セグメント利益)": number | null;
  "Employees (連結従業員数)": number | null;
}

export interface AnalyzeResponse {
  company_summary: CompanySummary;
  segment_details: SegmentDetail[];
  logs: string[];
}
