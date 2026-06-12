"use client";

import { motion } from "framer-motion";
import type { SegmentDetail } from "@/types";

interface SegmentDetailsProps {
  segments: SegmentDetail[];
}

function fmtNum(val: number | null): string {
  if (val === null || val === undefined) return "—";
  return val.toLocaleString("ja-JP");
}

export default function SegmentDetails({ segments }: SegmentDetailsProps) {
  if (segments.length === 0) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="glass-card p-8 text-center"
      >
        <span className="text-3xl">📭</span>
        <p className="mt-3 text-sm text-gray-400">
          No segment details found in this document.
        </p>
        <p className="mt-1 text-xs text-gray-600">
          This company may not report segment sales/profits or the data format
          differs from expected.
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
          <span className="text-lg">📊</span>
          <h2 className="text-base font-semibold text-gray-200">
            Segment Details
          </h2>
          <span className="rounded-full bg-sky-500/10 px-2.5 py-0.5 text-xs font-medium text-sky-400">
            {segments.length} segments
          </span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-white/5 text-xs font-medium uppercase tracking-wider text-gray-500">
              <th className="px-6 py-3">Segment Name</th>
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
                key={seg["Segment ID"]}
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="border-b border-white/3 text-gray-300 transition-colors hover:bg-white/3"
              >
                <td className="px-6 py-3 font-medium text-white">
                  {seg["Segment Name"]}
                  <span className="ml-2 text-[10px] text-gray-600">
                    {seg["Segment ID"]}
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(
                    seg[
                      "Sales to External Customers (外部顧客への売上高)"
                    ]
                  )}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(seg["Segment Profit (セグメント利益)"])}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">
                  {fmtNum(seg["Employees (連結従業員数)"])}
                </td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="border-t border-white/5 px-6 py-3">
        <p className="text-[10px] text-gray-600">
          ※ Values are shown exactly as extracted from the XBRL. Empty values
          mean the tag was not found for that segment context.
        </p>
      </div>
    </motion.div>
  );
}
