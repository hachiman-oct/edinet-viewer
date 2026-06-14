"use client";

import { useEffect, useState } from "react";
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
          setIsLoading(false);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err instanceof Error ? err.message : "データの取得に失敗しました。");
          setIsLoading(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [edinetCode]);

  // Get latest fiscal year's segments for pie charts
  const latestSegments = data?.segments
    ? (() => {
        const years = Object.keys(data.segments)
          .map(Number)
          .sort((a, b) => b - a);
        return years.length > 0 ? data.segments[years[0]] : [];
      })()
    : [];

  const latestFiscalYear = data?.segments
    ? (() => {
        const years = Object.keys(data.segments)
          .map(Number)
          .sort((a, b) => b - a);
        return years.length > 0 ? years[0] : null;
      })()
    : null;

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
            onClick={() => router.back()}
            className="group flex w-fit items-center gap-2 rounded-full border border-white/10 bg-white/5 pl-2 pr-4 py-1.5 text-sm text-gray-300 transition-colors hover:bg-white/10"
          >
            <div className="flex h-6 w-6 items-center justify-center rounded-full bg-white/10 transition-transform group-hover:-translate-x-1">
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M19 12H5M12 19l-7-7 7-7"/>
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
                <div className="h-8 w-8 animate-spin-slow rounded-full border-2 border-indigo-500/30 border-t-indigo-500 mb-4" />
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

                {/* Financial Highlights (latest period) */}
                {data.financials.length > 0 && (
                  <FinancialHighlights financial={data.financials[0]} />
                )}

                {/* Financial Timeline */}
                {data.financials.length > 1 && (
                  <FinancialTimeline financials={data.financials} />
                )}

                {/* Segment Details */}
                {latestSegments && latestSegments.length > 0 && (
                  <>
                    <SegmentDetails
                      segments={latestSegments}
                      fiscalYear={latestFiscalYear}
                    />
                    <PieCharts
                      segments={latestSegments}
                      fiscalYear={latestFiscalYear}
                    />
                  </>
                )}
              </motion.div>
            )}
          </AnimatePresence>
        </main>
      </div>
    </div>
  );
}
