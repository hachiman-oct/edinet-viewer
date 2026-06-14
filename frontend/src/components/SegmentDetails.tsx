"use client";

import { motion } from "framer-motion";
import type { SegmentDetail } from "@/types";

interface SegmentDetailsProps {
  segments: SegmentDetail[];
  fiscalYear: number | null;
}

function fmtNum(val: number | null): string {
  if (val === null || val === undefined) return "—";
  return val.toLocaleString("ja-JP");
}

export default function SegmentDetails({ segments, fiscalYear }: SegmentDetailsProps) {
  if (segments.length === 0) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="glass-card p-8 text-center"
      >
        <span className="text-3xl">📭</span>
        <p className="mt-3 text-sm text-gray-400">
          セグメント情報が見つかりませんでした。
        </p>
        <p className="mt-1 text-xs text-gray-600">
          この企業はセグメント別売上・利益を開示していないか、データ形式が異なる可能性があります。
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
      <div className="border-b border-white/5 px-6 py-4">
        <div className="flex items-center gap-2">
          <span className="text-lg">🧩</span>
          <h2 className="text-base font-semibold text-gray-200">
            セグメント情報
          </h2>
          <span className="rounded-full bg-sky-500/10 px-2.5 py-0.5 text-xs font-medium text-sky-400">
            {segments.length} セグメント
          </span>
          {fiscalYear && (
            <span className="rounded-full bg-white/5 px-2.5 py-0.5 text-xs text-gray-500">
              {fiscalYear}年度
            </span>
          )}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-white/5 text-xs font-medium uppercase tracking-wider text-gray-500">
              <th className="px-6 py-3">セグメント名</th>
              <th className="px-4 py-3 text-right">
                外部顧客への売上高
              </th>
              <th className="px-4 py-3 text-right">
                セグメント利益
              </th>
              <th className="px-4 py-3 text-right">
                連結従業員数
              </th>
            </tr>
          </thead>
          <tbody>
            {segments.map((seg, i) => (
              <motion.tr
                key={`${seg.segment_name}-${i}`}
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="border-b border-white/3 text-gray-300 transition-colors hover:bg-white/3"
              >
                <td className="px-6 py-3 font-medium text-white">
                  {seg.segment_name}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(seg.segment_revenue)}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(seg.segment_profit)}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(seg.segment_employees)}
                </td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="border-t border-white/5 px-6 py-3">
        <p className="text-[10px] text-gray-600">
          ※ 値はXBRLから抽出したデータをそのまま表示しています。空欄はタグが見つからなかったことを意味します。
        </p>
      </div>
    </motion.div>
  );
}
