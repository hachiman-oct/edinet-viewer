"use client";

import { useState, useCallback } from "react";
import { AnimatePresence, motion } from "framer-motion";

import Header from "@/components/Header";
import SearchForm from "@/components/SearchForm";
import ResultsTable from "@/components/ResultsTable";
import CompanySummary from "@/components/CompanySummary";
import SegmentDetails from "@/components/SegmentDetails";
import PieCharts from "@/components/PieCharts";
import { searchDocuments, analyzeDocument } from "@/lib/api";
import type { SearchResult, AnalyzeResponse } from "@/types";

export default function Home() {
  const [apiKey, setApiKey] = useState("");

  // Search state
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  // Analyze state
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null);
  const [analysisData, setAnalysisData] = useState<AnalyzeResponse | null>(
    null
  );
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analyzeError, setAnalyzeError] = useState<string | null>(null);

  // Logs
  const [showLogs, setShowLogs] = useState(false);

  const handleSearch = useCallback(
    async (filerName: string, periodEnd: string) => {
      setSearchError(null);
      setIsSearching(true);
      setAnalysisData(null);
      setSelectedDocId(null);
      setHasSearched(true);

      try {
        const data = await searchDocuments(filerName, periodEnd);
        setSearchResults(data.results);
      } catch (err) {
        setSearchError(
          err instanceof Error ? err.message : "An unexpected error occurred."
        );
        setSearchResults([]);
      } finally {
        setIsSearching(false);
      }
    },
    []
  );

  const handleAnalyze = useCallback(async () => {
    if (!selectedDocId) return;
    if (!apiKey) {
      setAnalyzeError(
        "Please provide an EDINET API Key in the header before analyzing."
      );
      return;
    }

    setAnalyzeError(null);
    setIsAnalyzing(true);
    setAnalysisData(null);

    try {
      const data = await analyzeDocument(selectedDocId, apiKey);
      setAnalysisData(data);
    } catch (err) {
      setAnalyzeError(
        err instanceof Error ? err.message : "An unexpected error occurred."
      );
    } finally {
      setIsAnalyzing(false);
    }
  }, [selectedDocId, apiKey]);

  const selectedRow = searchResults.find((r) => r.doc_id === selectedDocId);

  return (
    <div className="min-h-screen">
      {/* Background decoration */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -left-[400px] -top-[400px] h-[800px] w-[800px] rounded-full bg-indigo-500/5 blur-3xl" />
        <div className="absolute -bottom-[300px] -right-[300px] h-[600px] w-[600px] rounded-full bg-sky-500/5 blur-3xl" />
      </div>

      <div className="relative">
        <Header apiKey={apiKey} onApiKeyChange={setApiKey} />

        <main className="mx-auto max-w-7xl space-y-6 px-6 py-8">
          {/* Search Form */}
          <SearchForm onSearch={handleSearch} isLoading={isSearching} />

          {/* Error Messages */}
          <AnimatePresence>
            {searchError && (
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="glass-card border-rose-500/20 bg-rose-500/5 p-4 text-sm text-rose-400"
              >
                ⚠️ {searchError}
              </motion.div>
            )}
            {analyzeError && (
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="glass-card border-rose-500/20 bg-rose-500/5 p-4 text-sm text-rose-400"
              >
                ⚠️ {analyzeError}
              </motion.div>
            )}
          </AnimatePresence>

          {/* Search Results Table */}
          <AnimatePresence>
            {hasSearched && !isSearching && (
              <ResultsTable
                results={searchResults}
                selectedDocId={selectedDocId}
                onSelect={setSelectedDocId}
                onAnalyze={handleAnalyze}
                isAnalyzing={isAnalyzing}
              />
            )}
          </AnimatePresence>

          {/* Analysis Results */}
          <AnimatePresence>
            {analysisData && selectedRow && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="space-y-6"
              >
                {/* Debug Logs Toggle */}
                <div className="flex justify-end">
                  <button
                    type="button"
                    onClick={() => setShowLogs(!showLogs)}
                    className="rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-xs text-gray-400 transition-colors hover:bg-white/10 hover:text-gray-200"
                  >
                    {showLogs ? "🔽" : "▶️"} Debug Logs (
                    {analysisData.logs.length})
                  </button>
                </div>

                <AnimatePresence>
                  {showLogs && (
                    <motion.div
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: 1, height: "auto" }}
                      exit={{ opacity: 0, height: 0 }}
                      className="overflow-hidden"
                    >
                      <pre className="glass-card max-h-[300px] overflow-auto p-4 text-[11px] leading-relaxed text-gray-500">
                        {analysisData.logs.join("\n")}
                      </pre>
                    </motion.div>
                  )}
                </AnimatePresence>

                {/* Company Summary */}
                <CompanySummary
                  summary={analysisData.company_summary}
                  docId={selectedRow.doc_id}
                  filerName={selectedRow.filer_name}
                  periodEnd={selectedRow.period_end}
                  apiKey={apiKey}
                />

                {/* Segment Details Table */}
                <SegmentDetails segments={analysisData.segment_details} />

                {/* Pie Charts */}
                {analysisData.segment_details.length > 0 && (
                  <PieCharts segments={analysisData.segment_details} />
                )}
              </motion.div>
            )}
          </AnimatePresence>

          {/* Empty State */}
          {!hasSearched && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.3 }}
              className="flex flex-col items-center justify-center py-20"
            >
              <div className="mb-6 flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-indigo-500/10 to-sky-500/10 text-4xl">
                📊
              </div>
              <h2 className="mb-2 text-lg font-semibold text-gray-300">
                EDINET XBRL Viewer
              </h2>
              <p className="max-w-md text-center text-sm text-gray-500">
                Search for filings in BigQuery, download their XBRL data via
                EDINET API, and extract company summary and segment details.
              </p>
              <div className="mt-6 flex gap-3">
                {["BigQuery Search", "XBRL Download", "Segment Analysis"].map(
                  (step, i) => (
                    <div
                      key={step}
                      className="flex items-center gap-2 rounded-full bg-white/5 px-4 py-2 text-xs text-gray-400"
                    >
                      <span className="flex h-5 w-5 items-center justify-center rounded-full bg-indigo-500/20 text-[10px] font-bold text-indigo-400">
                        {i + 1}
                      </span>
                      {step}
                    </div>
                  )
                )}
              </div>
            </motion.div>
          )}
        </main>

        {/* Footer */}
        <footer className="border-t border-white/5 py-6 text-center text-xs text-gray-600">
          Powered by Next.js &amp; FastAPI | Data from EDINET API &amp; BigQuery
        </footer>
      </div>
    </div>
  );
}
