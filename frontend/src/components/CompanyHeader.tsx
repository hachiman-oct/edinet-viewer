"use client";

import { motion } from "framer-motion";
import type { CompanyInfo } from "@/types";

interface CompanyHeaderProps {
  company: CompanyInfo;
}

export default function CompanyHeader({ company }: CompanyHeaderProps) {
  const links = [
    ...(company.ticker_symbol
      ? [
          {
            label: "Yahoo!ファイナンス",
            href: `https://finance.yahoo.co.jp/quote/${company.ticker_symbol}.T`,
            className:
              "bg-indigo-500/10 text-indigo-300 hover:bg-indigo-500/20",
          },
          {
            label: "株探",
            href: `https://kabutan.jp/stock/?code=${company.ticker_symbol}`,
            className: "bg-sky-500/10 text-sky-300 hover:bg-sky-500/20",
          },
        ]
      : []),
    ...(company.jcn
      ? [
          {
            label: "gBizINFO",
            href: `https://info.gbiz.go.jp/hojin/ichiran?hojinBango=${company.jcn}`,
            className:
              "bg-emerald-400/10 text-emerald-400 hover:bg-emerald-400/20",
          },
        ]
      : []),
  ];

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.1 }}
      className="glass-card p-6"
    >
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        {/* Company name & info */}
        <div className="flex-1">
          <div className="mb-1 flex items-center gap-3">
            <span className="text-2xl">🏢</span>
            <div>
              <h2 className="text-xl font-bold text-white">
                {company.company_name_ja ?? company.edinet_code}
              </h2>
              {company.company_name_en && (
                <p className="text-sm text-gray-400">
                  {company.company_name_en}
                </p>
              )}
            </div>
          </div>
        </div>

        {/* Badges */}
        <div className="flex flex-wrap items-center gap-2">
          {company.ticker_symbol && (
            <span className="rounded-lg bg-indigo-500/10 px-3 py-1.5 font-mono text-sm font-medium text-indigo-400">
              {company.ticker_symbol}
            </span>
          )}
          <span className="rounded-lg bg-white/5 px-3 py-1.5 font-mono text-xs text-gray-400">
            {company.edinet_code}
          </span>
        </div>
      </div>

      {/* Details Grid */}
      <div className="mt-5 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div className="rounded-xl bg-white/3 p-3">
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wider text-gray-500">
            業種
          </p>
          <p className="text-sm font-medium text-gray-200">
            {company.industry ?? "—"}
          </p>
        </div>
        <div className="rounded-xl bg-white/3 p-3">
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wider text-gray-500">
            最新決算期
          </p>
          <p className="font-mono text-sm font-medium text-gray-200">
            {company.latest_period_end ?? "—"}
          </p>
        </div>
        <div className="rounded-xl bg-white/3 p-3">
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wider text-gray-500">
            法人番号
          </p>
          <p className="font-mono text-xs font-medium text-gray-200">
            {company.jcn ?? "—"}
          </p>
        </div>
        <div className="rounded-xl bg-white/3 p-3">
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wider text-gray-500">
            外部リンク
          </p>
          {links.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {links.map((link) => (
                <a
                  key={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={`rounded-md px-2 py-1 text-[10px] font-medium transition-colors ${link.className}`}
                >
                  {link.label} ↗
                </a>
              ))}
            </div>
          ) : (
            <p className="text-sm font-medium text-gray-500">—</p>
          )}
        </div>
      </div>
    </motion.div>
  );
}
