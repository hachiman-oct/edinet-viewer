// TypeScript type definitions for EDINET XBRL Viewer v2

// --- Company Search ---

export interface CompanySearchResult {
  edinet_code: string;
  ticker_symbol: string | null;
  company_name_ja: string | null;
  company_name_en: string | null;
  industry: string | null;
  jcn: string | null;
  latest_period_end: string | null;
}

export interface SearchResponse {
  results: CompanySearchResult[];
}

// --- Company Detail ---

export interface CompanyInfo {
  edinet_code: string;
  ticker_symbol: string | null;
  company_name_ja: string | null;
  company_name_en: string | null;
  industry: string | null;
  jcn: string | null;
  latest_period_end: string | null;
}

export interface FinancialSummary {
  fiscal_year: number | null;
  period_start: string | null;
  period_end: string | null;
  doc_id: string | null;
  submit_date: string | null;
  accounting_standard: string | null;
  revenue: number | null;
  net_income_loss: number | null;
  total_assets: number | null;
  net_assets: number | null;
  roe: number | null;
  pbr: number | null;
  per: number | null;
}

export interface SegmentDetail {
  segment_name: string;
  segment_revenue: number | null;
  segment_profit: number | null;
  segment_employees: number | null;
}

export interface CompanyDetailResponse {
  company: CompanyInfo;
  financials: FinancialSummary[];
  segments: Record<number, SegmentDetail[]>;
}
