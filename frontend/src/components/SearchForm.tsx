"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";

interface SearchFormProps {
  onSearch: (filerName: string, periodEnd: string) => void;
  isLoading: boolean;
  initialFilerName?: string;
  initialPeriodEnd?: string;
}

export default function SearchForm({
  onSearch,
  isLoading,
  initialFilerName = "",
  initialPeriodEnd = "",
}: SearchFormProps) {
  const [filerName, setFilerName] = useState(initialFilerName);
  const [periodEnd, setPeriodEnd] = useState(initialPeriodEnd);

  // Sync with external initial values (from URL params)
  useEffect(() => {
    setFilerName(initialFilerName);
  }, [initialFilerName]);
  useEffect(() => {
    setPeriodEnd(initialPeriodEnd);
  }, [initialPeriodEnd]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch(filerName, periodEnd);
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.1 }}
      className="glass-card p-6"
    >
      <div className="mb-5 flex items-center gap-2">
        <span className="text-lg">🔍</span>
        <h2 className="text-base font-semibold text-gray-200">
          Search Documents
        </h2>
      </div>

      <form onSubmit={handleSubmit} className="flex flex-col gap-4 sm:flex-row sm:items-end">
        <div className="flex-1">
          <label
            htmlFor="filer-name-input"
            className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-gray-500"
          >
            企業名 (Filer Name)
          </label>
          <input
            id="filer-name-input"
            type="text"
            value={filerName}
            onChange={(e) => setFilerName(e.target.value)}
            placeholder="例: トヨタ"
            className="input-field"
          />
        </div>

        <div className="flex-1">
          <label
            htmlFor="period-end-input"
            className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-gray-500"
          >
            決算日 (Period End)
          </label>
          <input
            id="period-end-input"
            type="date"
            value={periodEnd}
            onChange={(e) => setPeriodEnd(e.target.value)}
            className="input-field"
          />
        </div>

        <button
          type="submit"
          disabled={isLoading || (!filerName && !periodEnd)}
          className="btn-primary min-w-[140px]"
        >
          {isLoading ? (
            <>
              <span className="h-4 w-4 animate-spin-slow rounded-full border-2 border-white/30 border-t-white" />
              Searching…
            </>
          ) : (
            <>
              <span>🔎</span>
              Search
            </>
          )}
        </button>
      </form>
    </motion.div>
  );
}
