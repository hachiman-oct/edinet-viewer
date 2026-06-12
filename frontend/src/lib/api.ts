import type { SearchResponse, AnalyzeResponse } from "@/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export async function searchDocuments(
  filerName: string,
  periodEnd: string
): Promise<SearchResponse> {
  const params = new URLSearchParams();
  if (filerName) params.set("filer_name", filerName);
  if (periodEnd) params.set("period_end", periodEnd);

  const res = await fetch(`${API_BASE}/api/search?${params.toString()}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Search failed (status ${res.status})`);
  }
  return res.json();
}

export async function analyzeDocument(
  docId: string,
  apiKey: string
): Promise<AnalyzeResponse> {
  const res = await fetch(`${API_BASE}/api/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ doc_id: docId, api_key: apiKey }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Analysis failed (status ${res.status})`);
  }
  return res.json();
}
