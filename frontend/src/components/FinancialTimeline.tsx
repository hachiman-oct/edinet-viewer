"use client";

import { motion } from "framer-motion";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";
import type { FinancialSummary } from "@/types";
import {
  formatMillionYen,
  formatMillionNumber,
  formatPercent,
  formatRatio,
} from "@/lib/format";

interface FinancialTimelineProps {
  financials: FinancialSummary[];
  selectedYear?: number | null;
  onSelectYear?: (year: number) => void;
}

const tooltipStyle = {
  background: "rgba(15, 21, 53, 0.95)",
  border: "1px solid rgba(255,255,255,0.1)",
  borderRadius: "12px",
  padding: "10px 14px",
  fontSize: "12px",
  color: "#e5e7eb",
  boxShadow: "0 8px 32px rgba(0,0,0,0.4)",
};

export default function FinancialTimeline({
  financials,
  selectedYear,
  onSelectYear,
}: FinancialTimelineProps) {
  // Sort by fiscal year ascending for chart
  const chartData = [...financials]
    .sort((a, b) => (a.fiscal_year ?? 0) - (b.fiscal_year ?? 0))
    .map((f) => ({
      year: f.fiscal_year ? `${f.fiscal_year}` : "—",
      revenue: f.revenue,
      net_income_loss: f.net_income_loss,
      total_assets: f.total_assets,
      net_assets: f.net_assets,
    }));

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.2 }}
      className="glass-card p-6"
    >
      <div className="mb-5 flex items-center gap-2">
        <span className="text-lg">📈</span>
        <h2 className="text-base font-semibold text-gray-200">
          財務推移
        </h2>
        <span className="rounded-full bg-emerald-400/10 px-2.5 py-0.5 text-xs font-medium text-emerald-400">
          {chartData.length}期分
        </span>
      </div>

      {/* Revenue & Net Income Chart */}
      <div className="mb-6">
        <h3 className="mb-3 text-xs font-medium uppercase tracking-wider text-gray-500">
          売上高 / 純利益 (百万円)
        </h3>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={chartData} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
            <XAxis
              dataKey="year"
              tick={{ fill: "#6b7280", fontSize: 11 }}
              axisLine={{ stroke: "rgba(255,255,255,0.1)" }}
            />
            <YAxis
              tickFormatter={(v) => formatMillionNumber(v)}
              tick={{ fill: "#6b7280", fontSize: 11 }}
              axisLine={{ stroke: "rgba(255,255,255,0.1)" }}
              width={80}
            />
            <Tooltip
              contentStyle={tooltipStyle}
              formatter={(value: unknown, name: unknown) => {
                const label = name === "revenue" ? "売上高" : "純利益";
                const num = typeof value === "number" ? value : null;
                return [formatMillionYen(num), label];
              }}
            />
            <Legend
              formatter={(value: string) =>
                value === "revenue" ? "売上高" : "純利益"
              }
              wrapperStyle={{ fontSize: "11px", color: "#9ca3af" }}
            />
            <Bar
              dataKey="revenue"
              fill="#818cf8"
              radius={[4, 4, 0, 0]}
              name="revenue"
            />
            <Bar
              dataKey="net_income_loss"
              fill="#34d399"
              radius={[4, 4, 0, 0]}
              name="net_income_loss"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Assets Chart */}
      <div>
        <h3 className="mb-3 text-xs font-medium uppercase tracking-wider text-gray-500">
          総資産 / 純資産 (百万円)
        </h3>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={chartData} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
            <XAxis
              dataKey="year"
              tick={{ fill: "#6b7280", fontSize: 11 }}
              axisLine={{ stroke: "rgba(255,255,255,0.1)" }}
            />
            <YAxis
              tickFormatter={(v) => formatMillionNumber(v)}
              tick={{ fill: "#6b7280", fontSize: 11 }}
              axisLine={{ stroke: "rgba(255,255,255,0.1)" }}
              width={80}
            />
            <Tooltip
              contentStyle={tooltipStyle}
              formatter={(value: unknown, name: unknown) => {
                const label = name === "total_assets" ? "総資産" : "純資産";
                const num = typeof value === "number" ? value : null;
                return [formatMillionYen(num), label];
              }}
            />
            <Legend
              formatter={(value: string) =>
                value === "total_assets" ? "総資産" : "純資産"
              }
              wrapperStyle={{ fontSize: "11px", color: "#9ca3af" }}
            />
            <Bar
              dataKey="total_assets"
              fill="#38bdf8"
              radius={[4, 4, 0, 0]}
              name="total_assets"
            />
            <Bar
              dataKey="net_assets"
              fill="#fbbf24"
              radius={[4, 4, 0, 0]}
              name="net_assets"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Table */}
      <div className="mt-6 overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-white/5 text-xs font-medium uppercase tracking-wider text-gray-500">
              <th className="px-4 py-3">年度</th>
              <th className="px-4 py-3 text-right">売上高</th>
              <th className="px-4 py-3 text-right">純利益</th>
              <th className="px-4 py-3 text-right">総資産</th>
              <th className="px-4 py-3 text-right">純資産</th>
              <th className="px-4 py-3 text-right">ROE</th>
              <th className="px-4 py-3 text-right">PER</th>
              <th className="px-4 py-3 text-right">PBR</th>
            </tr>
          </thead>
          <tbody>
            {financials.map((f, i) => {
              const isSelected =
                selectedYear !== undefined &&
                selectedYear !== null &&
                f.fiscal_year === selectedYear;

              return (
                <motion.tr
                  key={f.doc_id ?? i}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.05 }}
                  onClick={() => {
                    if (f.fiscal_year && onSelectYear) {
                      onSelectYear(f.fiscal_year);
                    }
                  }}
                  className={`border-b border-white/3 text-gray-300 transition-colors ${
                    onSelectYear && f.fiscal_year ? "cursor-pointer" : ""
                  } ${
                    isSelected
                      ? "bg-indigo-500/10 text-white"
                      : "hover:bg-white/3"
                  }`}
                >
                  <td className="px-4 py-3 font-medium text-white">
                    <span className="inline-flex items-center gap-1.5">
                      {f.fiscal_year ?? "—"}
                      {isSelected && (
                        <span className="h-1.5 w-1.5 rounded-full bg-indigo-400" />
                      )}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatMillionYen(f.revenue)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatMillionYen(f.net_income_loss)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatMillionYen(f.total_assets)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatMillionYen(f.net_assets)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatPercent(f.roe)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatRatio(f.per)}
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs">
                    {formatRatio(f.pbr)}
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
