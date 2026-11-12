"use client";

import { motion } from "framer-motion";
import type { FinancialSummary } from "@/types";
import {
  formatMillionYenParts,
  formatRatio,
} from "@/lib/format";

interface FinancialHighlightsProps {
  financial: FinancialSummary;
  previousFinancial?: FinancialSummary | null;
}

interface YoYBadge {
  text: string;
  tone: "positive" | "negative" | "neutral";
}

function getYoYBadge(
  key: keyof FinancialSummary,
  current: number | null | undefined,
  prev: number | null | undefined
): YoYBadge | null {
  if (
    current === null ||
    current === undefined ||
    prev === null ||
    prev === undefined
  ) {
    return null;
  }

  // ROE: percentage point difference
  if (key === "roe") {
    const diffPt = (current - prev) * 100;
    const sign = diffPt > 0 ? "+" : "";
    return {
      text: `${sign}${diffPt.toFixed(2)}pt YoY`,
      tone: diffPt > 0 ? "positive" : diffPt < 0 ? "negative" : "neutral",
    };
  }

  // PER / PBR: skip YoY badge (valuation multiples)
  if (key === "per" || key === "pbr") {
    return null;
  }

  // Monetary metrics (revenue, net_income_loss, total_assets, net_assets)
  if (prev < 0 && current > 0) {
    return { text: "黒字転換", tone: "positive" };
  }
  if (prev > 0 && current < 0) {
    return { text: "赤字転落", tone: "negative" };
  }
  if (prev <= 0) {
    return null;
  }

  const diffPct = ((current - prev) / prev) * 100;
  const sign = diffPct > 0 ? "+" : "";
  return {
    text: `${sign}${diffPct.toFixed(1)}% YoY`,
    tone: diffPct > 0 ? "positive" : diffPct < 0 ? "negative" : "neutral",
  };
}

const metrics = [
  {
    key: "revenue",
    label: "売上高",
    icon: "💰",
    formatParts: (v: number | null | undefined) => formatMillionYenParts(v),
  },
  {
    key: "net_income_loss",
    label: "純利益",
    icon: "📈",
    formatParts: (v: number | null | undefined) => formatMillionYenParts(v),
  },
  {
    key: "total_assets",
    label: "総資産",
    icon: "🏦",
    formatParts: (v: number | null | undefined) => formatMillionYenParts(v),
  },
  {
    key: "net_assets",
    label: "純資産",
    icon: "💎",
    formatParts: (v: number | null | undefined) => formatMillionYenParts(v),
  },
  {
    key: "roe",
    label: "ROE",
    icon: "🎯",
    formatParts: (v: number | null | undefined) =>
      v === null || v === undefined
        ? { main: "N/A" }
        : { main: (v * 100).toFixed(2), unit: "%" },
  },
  {
    key: "per",
    label: "PER",
    icon: "📊",
    formatParts: (v: number | null | undefined) =>
      v === null || v === undefined
        ? { main: "N/A" }
        : { main: formatRatio(v), unit: "倍" },
  },
  {
    key: "pbr",
    label: "PBR",
    icon: "📉",
    formatParts: (v: number | null | undefined) =>
      v === null || v === undefined
        ? { main: "N/A" }
        : { main: formatRatio(v), unit: "倍" },
  },
] as const;

export default function FinancialHighlights({
  financial,
  previousFinancial,
}: FinancialHighlightsProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.15 }}
      className="glass-card p-6"
    >
      <div className="mb-5 flex flex-wrap items-center gap-2">
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
        {metrics.map(({ key, label, icon, formatParts }, i) => {
          const val = financial[key as keyof FinancialSummary] as
            | number
            | null;
          const prevVal = previousFinancial
            ? (previousFinancial[key as keyof FinancialSummary] as
                | number
                | null)
            : null;
          const yoy = getYoYBadge(
            key as keyof FinancialSummary,
            val,
            prevVal
          );
          const { main, unit } = formatParts(val);

          return (
            <motion.div
              key={key}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 0.1 + i * 0.05 }}
              className="glass-card glass-card-hover flex flex-col justify-between p-4"
            >
              <div className="mb-2 flex flex-wrap items-center justify-between gap-1.5">
                <div className="flex items-center gap-1.5">
                  <span className="text-base">{icon}</span>
                  <p className="text-[11px] font-medium uppercase tracking-wider text-gray-400">
                    {label}
                  </p>
                </div>
                {yoy && (
                  <span
                    className={`rounded-full px-2 py-0.5 font-mono text-[10px] font-medium ${
                      yoy.tone === "positive"
                        ? "bg-emerald-400/10 text-emerald-400"
                        : yoy.tone === "negative"
                        ? "bg-rose-400/10 text-rose-400"
                        : "bg-white/5 text-gray-400"
                    }`}
                  >
                    {yoy.text}
                  </span>
                )}
              </div>
              <p className="font-mono text-lg font-bold tracking-tight text-white sm:text-xl">
                {main}
                {unit && (
                  <span className="ml-1 font-sans text-xs font-medium text-gray-400">
                    {unit}
                  </span>
                )}
              </p>
            </motion.div>
          );
        })}
      </div>
    </motion.div>
  );
}
