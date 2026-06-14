"use client";

import { useState, useEffect, useCallback, useRef, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { AnimatePresence, motion } from "framer-motion";
import Link from "next/link";

import Header from "@/components/Header";
import { searchCompanies } from "@/lib/api";
import type { CompanySearchResult } from "@/types";

// ---------------------------------------------------------------------------
// Inner component (needs useSearchParams inside Suspense)
// ---------------------------------------------------------------------------

function SearchInner() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const initialQuery = searchParams.get("q") || "";

  const [query, setQuery] = useState(initialQuery);
  const [results, setResults] = useState<CompanySearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  const didAutoSearch = useRef(false);

  const doSearch = useCallback(async (q: string) => {
    const trimmed = q.trim();
    if (!trimmed) return;

    setError(null);
    setIsSearching(true);
    setHasSearched(true);

    // Update URL
    const url = new URL(window.location.href);
    url.searchParams.set("q", trimmed);
    window.history.replaceState({}, "", url.toString());

    try {
      const data = await searchCompanies(trimmed);
      setResults(data.results);
    } catch (err) {
      setError(err instanceof Error ? err.message : "検索中にエラーが発生しました。");
      setResults([]);
    } finally {
      setIsSearching(false);
    }
  }, []);

  // Auto-search from URL params
  useEffect(() => {
    if (didAutoSearch.current) return;
    if (initialQuery) {
      didAutoSearch.current = true;
      doSearch(initialQuery);
    }
  }, [initialQuery, doSearch]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    doSearch(query);
  };

  return (
    <div className="min-h-screen">
      {/* Background decoration */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -left-[400px] -top-[400px] h-[800px] w-[800px] rounded-full bg-indigo-500/5 blur-3xl" />
        <div className="absolute -bottom-[300px] -right-[300px] h-[600px] w-[600px] rounded-full bg-sky-500/5 blur-3xl" />
      </div>

      <div className="relative">
        <Header />

        <main className="mx-auto max-w-7xl space-y-6 px-6 py-8">
          {/* Search Form */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="glass-card p-6"
          >
            <div className="mb-5 flex items-center gap-2">
              <span className="text-lg">🔍</span>
              <h2 className="text-base font-semibold text-gray-200">
                企業検索
              </h2>
            </div>

            <form onSubmit={handleSubmit} className="flex gap-3">
              <input
                id="search-query-input"
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="企業名・銘柄コード・EDINETコードで検索..."
                autoComplete="off"
                data-1p-ignore
                className="input-field flex-1"
              />
              <button
                type="submit"
                disabled={isSearching || !query.trim()}
                className="btn-primary min-w-[120px]"
              >
                {isSearching ? (
                  <>
                    <span className="h-4 w-4 animate-spin-slow rounded-full border-2 border-white/30 border-t-white" />
                    検索中…
                  </>
                ) : (
                  <>
                    <span>🔎</span>
                    検索
                  </>
                )}
              </button>
            </form>
          </motion.div>

          {/* Error */}
          <AnimatePresence>
            {error && (
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="glass-card border-rose-500/20 bg-rose-500/5 p-4 text-sm text-rose-400"
              >
                ⚠️ {error}
              </motion.div>
            )}
          </AnimatePresence>

          {/* Results */}
          <AnimatePresence>
            {hasSearched && !isSearching && (
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.5, delay: 0.2 }}
              >
                {results.length === 0 ? (
                  <div className="glass-card p-8 text-center">
                    <span className="text-3xl">📭</span>
                    <p className="mt-3 text-sm text-gray-400">
                      検索条件に一致する企業が見つかりませんでした。
                    </p>
                  </div>
                ) : (
                  <div className="glass-card overflow-hidden">
                    <div className="flex items-center justify-between border-b border-white/5 px-6 py-4">
                      <div className="flex items-center gap-2">
                        <span className="text-lg">🏢</span>
                        <h2 className="text-base font-semibold text-gray-200">
                          検索結果
                        </h2>
                        <span className="rounded-full bg-indigo-500/10 px-2.5 py-0.5 text-xs font-medium text-indigo-400">
                          {results.length}
                        </span>
                      </div>
                    </div>

                    <div className="max-h-[500px] overflow-auto">
                      <table className="w-full text-left text-sm">
                        <thead className="sticky top-0 z-10 bg-navy-900/90 backdrop-blur-sm">
                          <tr className="border-b border-white/5 text-xs font-medium uppercase tracking-wider text-gray-500">
                            <th className="px-6 py-3">企業名</th>
                            <th className="px-4 py-3">銘柄コード</th>
                            <th className="px-4 py-3">業種</th>
                            <th className="px-4 py-3">最新決算期</th>
                            <th className="px-4 py-3">EDINET</th>
                          </tr>
                        </thead>
                        <tbody>
                          {results.map((r, i) => (
                            <motion.tr
                              key={r.edinet_code}
                              initial={{ opacity: 0, x: -10 }}
                              animate={{ opacity: 1, x: 0 }}
                              transition={{ delay: i * 0.03 }}
                              className="cursor-pointer border-b border-white/3 text-gray-300 transition-colors hover:bg-white/5"
                              onClick={() =>
                                router.push(`/companies/${r.edinet_code}`)
                              }
                            >
                              <td className="px-6 py-3">
                                <div className="font-medium text-white">
                                  {r.company_name_ja ?? "—"}
                                </div>
                                {r.company_name_en && (
                                  <div className="mt-0.5 text-[11px] text-gray-500">
                                    {r.company_name_en}
                                  </div>
                                )}
                              </td>
                              <td className="px-4 py-3 font-mono text-xs text-gray-400">
                                {r.ticker_symbol ?? "—"}
                              </td>
                              <td className="px-4 py-3 text-xs text-gray-400">
                                {r.industry ?? "—"}
                              </td>
                              <td className="px-4 py-3 font-mono text-xs text-gray-400">
                                {r.latest_period_end ?? "—"}
                              </td>
                              <td className="px-4 py-3 font-mono text-xs text-gray-500">
                                {r.edinet_code}
                              </td>
                            </motion.tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </motion.div>
            )}
          </AnimatePresence>
        </main>

        {/* Footer */}
        <footer className="border-t border-white/5 py-6 text-center text-xs text-gray-600">
          Powered by Next.js &amp; FastAPI | Data from EDINET
        </footer>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page export (wraps in Suspense for useSearchParams)
// ---------------------------------------------------------------------------

export default function SearchPage() {
  return (
    <Suspense
      fallback={
        <div className="flex min-h-screen items-center justify-center text-gray-500">
          Loading…
        </div>
      }
    >
      <SearchInner />
    </Suspense>
  );
}
