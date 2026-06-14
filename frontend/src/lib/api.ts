import type { SearchResponse, CompanyDetailResponse } from "@/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export async function searchCompanies(
  query: string
): Promise<SearchResponse> {
  const params = new URLSearchParams();
  params.set("q", query);

  const res = await fetch(`${API_BASE}/api/search?${params.toString()}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Search failed (status ${res.status})`);
  }
  return res.json();
}

export async function getCompanyDetail(
  edinetCode: string
): Promise<CompanyDetailResponse> {
  const res = await fetch(`${API_BASE}/api/companies/${edinetCode}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Failed to load company (status ${res.status})`);
  }
  return res.json();
}
