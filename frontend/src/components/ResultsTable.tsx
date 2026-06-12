"use client";

import { motion } from "framer-motion";
import type { SearchResult } from "@/types";

interface ResultsTableProps {
  results: SearchResult[];
  selectedDocId: string | null;
  onSelect: (docId: string) => void;
  onAnalyze: () => void;
  isAnalyzing: boolean;
}

export default function ResultsTable({
  results,
  selectedDocId,
  onSelect,
  onAnalyze,
  isAnalyzing,
}: ResultsTableProps) {
  if (results.length === 0) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="glass-card p-8 text-center"
      >
        <span className="text-3xl">📭</span>
        <p className="mt-3 text-sm text-gray-400">
          No documents found matching the search criteria.
        </p>
      </motion.div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.2 }}
      className="glass-card overflow-hidden"
    >
      <div className="flex items-center justify-between border-b border-white/5 px-6 py-4">
        <div className="flex items-center gap-2">
          <span className="text-lg">📋</span>
          <h2 className="text-base font-semibold text-gray-200">
            Search Results
          </h2>
          <span className="rounded-full bg-indigo-500/10 px-2.5 py-0.5 text-xs font-medium text-indigo-400">
            {results.length}
          </span>
        </div>

        <button
          type="button"
          onClick={onAnalyze}
          disabled={!selectedDocId || isAnalyzing}
          className="btn-primary text-xs"
        >
          {isAnalyzing ? (
            <>
              <span className="h-3.5 w-3.5 animate-spin-slow rounded-full border-2 border-white/30 border-t-white" />
              Analyzing…
            </>
          ) : (
            <>
              <span>⚡</span>
              Download &amp; Analyze XBRL
            </>
          )}
        </button>
      </div>

      <div className="max-h-[400px] overflow-auto">
        <table className="w-full text-left text-sm">
          <thead className="sticky top-0 bg-navy-900/90 backdrop-blur-sm">
            <tr className="border-b border-white/5 text-xs font-medium uppercase tracking-wider text-gray-500">
              <th className="px-6 py-3">企業名</th>
              <th className="px-4 py-3">決算日</th>
              <th className="px-4 py-3">提出日</th>
              <th className="px-4 py-3">書類種別</th>
              <th className="px-4 py-3">Doc ID</th>
            </tr>
          </thead>
          <tbody>
            {results.map((r, i) => {
              const isSelected = selectedDocId === r.doc_id;
              return (
                <motion.tr
                  key={`${r.doc_id}-${i}`}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.03 }}
                  onClick={() => onSelect(r.doc_id)}
                  className={`cursor-pointer border-b border-white/3 transition-colors ${
                    isSelected
                      ? "bg-indigo-500/10 text-white"
                      : "text-gray-300 hover:bg-white/3"
                  }`}
                >
                  <td className="px-6 py-3 font-medium">{r.filer_name}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-400">
                    {r.period_end ?? "—"}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-400">
                    {r.submit_date_time?.split(" ")[0] ?? "—"}
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-400">
                    {r.doc_description}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-500">
                    {r.doc_id}
                  </td>
                </motion.tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </motion.div>
  );
}
