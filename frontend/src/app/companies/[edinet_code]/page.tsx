"use client";

import { useEffect, useState, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";
import { AnimatePresence, motion } from "framer-motion";

import Header from "@/components/Header";
import CompanyHeader from "@/components/CompanyHeader";
import FinancialHighlights from "@/components/FinancialHighlights";
import FinancialTimeline from "@/components/FinancialTimeline";
import SegmentDetails from "@/components/SegmentDetails";
import PieCharts from "@/components/PieCharts";
import { getCompanyDetail } from "@/lib/api";
import type { CompanyDetailResponse } from "@/types";

export default function CompanyPage() {
  const params = useParams<{ edinet_code: string }>();
  const router = useRouter();
  const edinetCode = params.edinet_code;

  const [data, setData] = useState<CompanyDetailResponse | null>(null);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!edinetCode) return;

    let isMounted = true;
    setIsLoading(true);
    setError(null);

    getCompanyDetail(edinetCode)
      .then((res) => {
        if (isMounted) {
          setData(res);
          // Initialize selectedYear to latest available fiscal year
          const finYears = res.financials
            .map((f) => f.fiscal_year)
            .filter((y): y is number => y !== null);
          const segYears = Object.keys(res.segments)
            .map(Number)
            .filter((y) => y > 0);
          const allYears = Array.from(new Set([...finYears, ...segYears])).sort(
            (a, b) => b - a
          );
          setSelectedYear(allYears.length > 0 ? allYears[0] : null);
          setIsLoading(false);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(
            err instanceof Error ? err.message : "データの取得に失敗しました。"
          );
          setIsLoading(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [edinetCode]);

  // All available fiscal years (descending)
  const availableYears = useMemo(() => {
    if (!data) return [];
    const finYears = data.financials
      .map((f) => f.fiscal_year)
      .filter((y): y is number => y !== null);
    const segYears = Object.keys(data.segments)
      .map(Number)
      .filter((y) => y > 0);
    return Array.from(new Set([...finYears, ...segYears])).sort(
      (a, b) => b - a
    );
  }, [data]);

  // Financial summary for the selected year (and previous year for YoY)
  const { selectedFinancial, previousFinancial } = useMemo(() => {
    if (!data || data.financials.length === 0) {
      return { selectedFinancial: null, previousFinancial: null };
    }
    const idx =
      selectedYear !== null
        ? data.financials.findIndex((f) => f.fiscal_year === selectedYear)
        : 0;
    const currentIdx = idx >= 0 ? idx : 0;
    const current = data.financials[currentIdx];

    // Look for exact (fiscal_year - 1), or fallback to next older record
    let prev = null;
    if (current.fiscal_year !== null) {
      prev =
        data.financials.find(
          (f) => f.fiscal_year === current.fiscal_year! - 1
        ) ?? null;
    }
    if (!prev && currentIdx + 1 < data.financials.length) {
      prev = data.financials[currentIdx + 1];
    }

    return { selectedFinancial: current, previousFinancial: prev };
  }, [data, selectedYear]);

  // Segments for the selected year
  const selectedSegments = useMemo(() => {
    if (!data?.segments) return [];
    if (selectedYear !== null && data.segments[selectedYear]) {
      return data.segments[selectedYear];
    }
    return [];
  }, [data, selectedYear]);

  const handleBack = () => {
    if (typeof window !== "undefined" && window.history.length > 1) {
      router.back();
    } else {
      router.push("/search");
    }
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
          {/* Back Button */}
          <button
            onClick={handleBack}
            className="group flex w-fit items-center gap-2 rounded-full border border-white/10 bg-white/5 py-1.5 pl-2 pr-4 text-sm text-gray-300 transition-colors hover:bg-white/10"
          >
            <div className="flex h-6 w-6 items-center justify-center rounded-full bg-white/10 transition-transform group-hover:-translate-x-1">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M19 12H5M12 19l-7-7 7-7" />
              </svg>
            </div>
            検索結果に戻る
          </button>

          <AnimatePresence mode="wait">
            {isLoading && (
              <motion.div
                key="loading"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex flex-col items-center justify-center py-20"
              >
                <div className="mb-4 h-8 w-8 animate-spin-slow rounded-full border-2 border-indigo-500/30 border-t-indigo-500" />
                <p className="text-sm text-gray-400">
                  {edinetCode} のデータを取得中...
                </p>
              </motion.div>
            )}

            {error && (
              <motion.div
                key="error"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="glass-card border-rose-500/20 bg-rose-500/5 p-4 text-sm text-rose-400"
              >
                ⚠️ {error}
              </motion.div>
            )}

            {data && !isLoading && (
              <motion.div
                key="content"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="space-y-6"
              >
                {/* Company Header */}
                <CompanyHeader company={data.company} />

                {/* Fiscal Year Selector */}
                {availableYears.length > 0 && (
                  <div className="glass-card flex flex-wrap items-center justify-between gap-3 px-6 py-4">
                    <div className="flex items-center gap-2">
                      <span className="text-base">📅</span>
                      <span className="text-sm font-medium text-gray-300">
                        表示年度
                      </span>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      {availableYears.map((year) => {
                        const isActive = year === selectedYear;
                        return (
                          <button
                            key={year}
                            type="button"
                            onClick={() => setSelectedYear(year)}
                            className={`rounded-xl px-4 py-1.5 font-mono text-xs font-semibold transition-all ${
                              isActive
                                ? "bg-gradient-to-r from-indigo-500 to-sky-500 text-white shadow-md shadow-indigo-500/25"
                                : "bg-white/5 text-gray-400 hover:bg-white/10 hover:text-gray-200"
                            }`}
                          >
                            {year}年度
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* Financial Highlights (for selected fiscal year) */}
                {selectedFinancial && (
                  <FinancialHighlights
                    financial={selectedFinancial}
                    previousFinancial={previousFinancial}
                  />
                )}

                {/* Financial Timeline */}
                {data.financials.length > 1 && (
                  <FinancialTimeline
                    financials={data.financials}
                    selectedYear={selectedYear}
                    onSelectYear={setSelectedYear}
                  />
                )}

                {/* Segment Details & Pie Charts (for selected fiscal year) */}
                <SegmentDetails
                  segments={selectedSegments}
                  fiscalYear={selectedYear}
                />
                {selectedSegments.length > 0 && (
                  <PieCharts
                    segments={selectedSegments}
                    fiscalYear={selectedYear}
                  />
                )}
              </motion.div>
            )}
          </AnimatePresence>
        </main>
      </div>
    </div>
  );
}
