"use client";

import { motion } from "framer-motion";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import type { SegmentDetail } from "@/types";

interface PieChartsProps {
  segments: SegmentDetail[];
  fiscalYear: number | null;
}

const COLORS = [
  "#818cf8", // indigo-400
  "#38bdf8", // sky-400
  "#34d399", // emerald-400
  "#fbbf24", // amber-400
  "#fb7185", // rose-400
  "#a78bfa", // violet-400
  "#2dd4bf", // teal-400
  "#f472b6", // pink-400
  "#60a5fa", // blue-400
  "#4ade80", // green-400
];

interface ChartData {
  name: string;
  value: number;
}

function buildChartData(
  segments: SegmentDetail[],
  accessor: (s: SegmentDetail) => number | null
): ChartData[] {
  return segments
    .map((s) => ({
      name: s.segment_name,
      value: accessor(s),
    }))
    .filter((d): d is ChartData => d.value !== null && d.value > 0);
}

const tooltipContentStyle = {
  background: "rgba(15, 21, 53, 0.95)",
  border: "1px solid rgba(255,255,255,0.1)",
  borderRadius: "12px",
  padding: "10px 14px",
  fontSize: "12px",
  color: "#e5e7eb",
  boxShadow: "0 8px 32px rgba(0,0,0,0.4)",
};

interface SinglePieProps {
  title: string;
  emoji: string;
  data: ChartData[];
  delay: number;
}

function SinglePie({ title, emoji, data, delay }: SinglePieProps) {
  if (data.length === 0) {
    return (
      <div className="glass-card flex flex-col items-center justify-center p-6 text-center">
        <span className="text-2xl">{emoji}</span>
        <p className="mt-2 text-sm font-medium text-gray-300">{title}</p>
        <p className="mt-1 text-xs text-gray-600">データなし</p>
      </div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.5, delay }}
      className="glass-card p-5"
    >
      <div className="mb-3 flex items-center gap-2">
        <span className="text-base">{emoji}</span>
        <h3 className="text-sm font-semibold text-gray-300">{title}</h3>
      </div>

      <ResponsiveContainer width="100%" height={260}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={50}
            outerRadius={90}
            paddingAngle={3}
            dataKey="value"
            stroke="none"
            animationDuration={800}
          >
            {data.map((_, index) => (
              <Cell
                key={`cell-${index}`}
                fill={COLORS[index % COLORS.length]}
                style={{ filter: "drop-shadow(0 0 6px rgba(0,0,0,0.3))" }}
              />
            ))}
          </Pie>
          <Tooltip
            formatter={(value: unknown) => {
              if (typeof value === "number") return value.toLocaleString("ja-JP");
              return String(value ?? "—");
            }}
            contentStyle={tooltipContentStyle}
            itemStyle={{ color: "#e5e7eb" }}
          />
          <Legend
            verticalAlign="bottom"
            iconType="circle"
            iconSize={8}
            wrapperStyle={{
              fontSize: "11px",
              color: "#9ca3af",
              paddingTop: "12px",
            }}
          />
        </PieChart>
      </ResponsiveContainer>
    </motion.div>
  );
}

export default function PieCharts({ segments, fiscalYear }: PieChartsProps) {
  const salesData = buildChartData(segments, (s) => s.segment_revenue);
  const profitData = buildChartData(segments, (s) => s.segment_profit);
  const empData = buildChartData(segments, (s) => s.segment_employees);

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.3 }}
    >
      <div className="mb-4 flex items-center gap-2">
        <span className="text-lg">📈</span>
        <h2 className="text-base font-semibold text-gray-200">
          セグメント構成比
        </h2>
        {fiscalYear && (
          <span className="rounded-full bg-white/5 px-2.5 py-0.5 text-xs text-gray-500">
            {fiscalYear}年度
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <SinglePie
          title="売上高"
          emoji="💰"
          data={salesData}
          delay={0.1}
        />
        <SinglePie
          title="利益"
          emoji="📈"
          data={profitData}
          delay={0.2}
        />
        <SinglePie
          title="従業員数"
          emoji="👥"
          data={empData}
          delay={0.3}
        />
      </div>
    </motion.div>
  );
}
