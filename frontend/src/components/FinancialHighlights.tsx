"use client";

import { motion } from "framer-motion";
import type { FinancialSummary } from "@/types";

interface FinancialHighlightsProps {
  financial: FinancialSummary;
}

function formatValue(val: number | null | undefined): string {
  if (val === null || val === undefined) return "N/A";
  // Display in 百万円 (million yen)
  if (Math.abs(val) >= 1_000_000) {
    return `${(val / 1_000_000).toLocaleString("ja-JP", { maximumFractionDigits: 0 })}百万`;
  }
  return val.toLocaleString("ja-JP");
}

function formatPercent(val: number | null | undefined): string {
  if (val === null || val === undefined) return "N/A";
  return `${(val * 100).toFixed(2)}%`;
}

function formatRatio(val: number | null | undefined): string {
  if (val === null || val === undefined) return "N/A";
  return val.toFixed(2);
}

const metrics = [
  { key: "revenue", label: "売上高", icon: "💰", format: formatValue },
  { key: "net_income_loss", label: "純利益", icon: "📈", format: formatValue },
  { key: "total_assets", label: "総資産", icon: "🏦", format: formatValue },
  { key: "net_assets", label: "純資産", icon: "💎", format: formatValue },
  { key: "roe", label: "ROE", icon: "🎯", format: formatPercent },
  { key: "per", label: "PER", icon: "📊", format: formatRatio },
  { key: "pbr", label: "PBR", icon: "📉", format: formatRatio },
] as const;

export default function FinancialHighlights({
  financial,
}: FinancialHighlightsProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.15 }}
      className="glass-card p-6"
    >
      <div className="mb-5 flex items-center gap-2">
        <span className="text-lg">📋</span>
        <h2 className="text-base font-semibold text-gray-200">
          財務ハイライト
        </h2>
        <span className="rounded-full bg-sky-500/10 px-2.5 py-0.5 text-xs font-medium text-sky-400">
          {financial.fiscal_year ?? "—"}年度
        </span>
        {financial.accounting_standard && (
          <span className="rounded-full bg-white/5 px-2.5 py-0.5 text-xs text-gray-500">
            {financial.accounting_standard}
          </span>
        )}
      </div>

      {/* Period info */}
      <div className="mb-4 text-xs text-gray-500">
        期間: {financial.period_start ?? "—"} 〜 {financial.period_end ?? "—"}
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
        {metrics.map(({ key, label, icon, format }, i) => {
          const val = financial[key as keyof FinancialSummary] as
            | number
            | null;
          return (
            <motion.div
              key={key}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 0.1 + i * 0.05 }}
              className="glass-card glass-card-hover p-4"
            >
              <div className="mb-1 flex items-center gap-1.5">
                <span className="text-base">{icon}</span>
                <p className="text-[10px] font-medium uppercase tracking-wider text-gray-500">
                  {label}
                </p>
              </div>
              <p className="text-lg font-bold text-white">{format(val)}</p>
            </motion.div>
          );
        })}
      </div>
    </motion.div>
  );
}
