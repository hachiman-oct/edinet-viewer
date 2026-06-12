"use client";

import { motion } from "framer-motion";
import type { CompanySummary as CompanySummaryType } from "@/types";

interface CompanySummaryProps {
  summary: CompanySummaryType;
  docId: string;
  filerName: string;
  periodEnd: string | null;
}

function formatMetricValue(key: string, val: string | number | null): string {
  if (val === null || val === undefined || val === "") return "N/A";
  if (typeof val === "string") return val;
  if (key.includes("ROE")) return `${(val * 100).toFixed(2)}%`;
  if (key.includes("PER") || key.includes("PBR")) return val.toFixed(2);
  return val.toLocaleString("ja-JP");
}

const metricIcons: Record<string, string> = {
  "Accounting Standards (会計基準)": "📏",
  "Net Sales (売上高)": "💰",
  "Net Income (純利益)": "📈",
  "Total Assets (総資産)": "🏦",
  "PER (株価収益率)": "📊",
  "PBR (株価純資産倍率)": "📉",
  "ROE (自己資本利益率)": "🎯",
};

export default function CompanySummary({
  summary,
  docId,
  filerName,
  periodEnd,
}: CompanySummaryProps) {
  const periodStart = summary["Period Start (期首)"];

  // Separate Period Start from the financial metrics
  const financialMetrics = Object.entries(summary).filter(
    ([key]) => key !== "Period Start (期首)"
  );

  const htmlUrl = `https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?${docId}`;
  const pdfUrl = `https://disclosure2dl.edinet-fsa.go.jp/searchdocument/pdf/${docId}.pdf`;

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.1 }}
      className="glass-card p-6"
    >
      {/* Header */}
      <div className="mb-6 flex items-center gap-2">
        <span className="text-lg">🏢</span>
        <h2 className="text-base font-semibold text-gray-200">
          Company Summary
        </h2>
      </div>

      {/* Company Info Cards */}
      <div className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div className="rounded-xl bg-white/3 p-4">
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between">
              <dt className="text-gray-500">Doc ID</dt>
              <dd className="font-mono text-xs text-gray-300">{docId}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-gray-500">Filer Name</dt>
              <dd className="font-medium text-white">{filerName}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-gray-500">Period</dt>
              <dd className="text-gray-300">
                {periodStart ?? "N/A"} ~ {periodEnd ?? "N/A"}
              </dd>
            </div>
          </dl>
        </div>

        <div className="rounded-xl bg-white/3 p-4">
          <p className="mb-2 text-xs font-medium uppercase tracking-wider text-gray-500">
            Links
          </p>
          <div className="flex flex-wrap gap-2">
            {[
              { label: "HTML", href: htmlUrl, color: "indigo" },
              { label: "PDF", href: pdfUrl, color: "rose" },
            ].map((link) => (
              <a
                key={link.label}
                href={link.href}
                target="_blank"
                rel="noopener noreferrer"
                className={`rounded-lg bg-${link.color}-400/10 px-3 py-1.5 text-xs font-medium text-${link.color}-400 transition-colors hover:bg-${link.color}-400/20`}
              >
                {link.label} ↗
              </a>
            ))}
          </div>
        </div>
      </div>

      {/* Financial Highlights */}
      <h3 className="mb-4 text-sm font-semibold text-gray-300">
        Financial Highlights
      </h3>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
        {financialMetrics.map(([key, val], i) => (
          <motion.div
            key={key}
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.1 + i * 0.05 }}
            className="glass-card glass-card-hover p-4"
          >
            <div className="mb-1 flex items-center gap-1.5">
              <span className="text-base">{metricIcons[key] ?? "📌"}</span>
              <p className="text-[10px] font-medium uppercase tracking-wider text-gray-500 leading-tight">
                {key.split("(")[0].trim()}
              </p>
            </div>
            <p className="text-lg font-bold text-white">
              {formatMetricValue(key, val)}
            </p>
            {key.includes("(") && (
              <p className="mt-0.5 text-[10px] text-gray-600">
                {key.match(/\((.+)\)/)?.[1]}
              </p>
            )}
          </motion.div>
        ))}
      </div>
    </motion.div>
  );
}
