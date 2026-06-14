"use client";

import { useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";

import Header from "@/components/Header";

export default function Home() {
  const router = useRouter();
  const [query, setQuery] = useState("");

  const handleSearch = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      const trimmed = query.trim();
      if (!trimmed) return;
      router.push(`/search?q=${encodeURIComponent(trimmed)}`);
    },
    [query, router]
  );

  return (
    <div className="min-h-screen">
      {/* Background decoration */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -left-[400px] -top-[400px] h-[800px] w-[800px] rounded-full bg-indigo-500/5 blur-3xl" />
        <div className="absolute -bottom-[300px] -right-[300px] h-[600px] w-[600px] rounded-full bg-sky-500/5 blur-3xl" />
        <div className="absolute left-1/2 top-1/3 h-[500px] w-[500px] -translate-x-1/2 rounded-full bg-emerald-500/3 blur-3xl" />
      </div>

      <div className="relative">
        <Header />

        <main className="mx-auto max-w-4xl px-6">
          {/* Hero Section */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.7 }}
            className="flex flex-col items-center justify-center pb-16 pt-24"
          >
            {/* Icon */}
            <motion.div
              initial={{ scale: 0.8, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={{ delay: 0.2, type: "spring", stiffness: 200 }}
              className="mb-8 flex h-24 w-24 items-center justify-center rounded-3xl bg-gradient-to-br from-indigo-500/15 to-sky-500/15 text-5xl shadow-2xl shadow-indigo-500/10"
            >
              📊
            </motion.div>

            {/* Title */}
            <h2 className="mb-3 text-center text-3xl font-bold tracking-tight sm:text-4xl">
              <span className="gradient-text">EDINET XBRL Viewer</span>
            </h2>
            <p className="mb-10 max-w-lg text-center text-base text-gray-400 leading-relaxed">
              有価証券報告書のXBRLデータから企業の財務サマリとセグメント情報を
              抽出・可視化するツールです。
            </p>

            {/* Search Form */}
            <motion.form
              onSubmit={handleSearch}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4, duration: 0.5 }}
              className="w-full max-w-xl"
            >
              <div className="glass-card flex items-center gap-3 p-2 pr-2">
                <span className="pl-3 text-lg text-gray-500">🔍</span>
                <input
                  id="home-search-input"
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="企業名・銘柄コード・EDINETコードで検索..."
                  autoComplete="off"
                  data-1p-ignore
                  className="flex-1 bg-transparent px-1 py-3 text-base text-gray-100 placeholder-gray-500 outline-none"
                />
                <button
                  type="submit"
                  disabled={!query.trim()}
                  className="btn-primary min-w-[100px] rounded-xl px-5 py-3"
                >
                  検索
                </button>
              </div>
            </motion.form>

            {/* Quick Examples */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.7 }}
              className="mt-6 flex flex-wrap items-center justify-center gap-2"
            >
              <span className="text-xs text-gray-600">例:</span>
              {["トヨタ", "7203", "ソニー", "E02529"].map((example) => (
                <button
                  key={example}
                  type="button"
                  onClick={() => {
                    setQuery(example);
                    router.push(`/search?q=${encodeURIComponent(example)}`);
                  }}
                  className="rounded-full bg-white/5 px-3 py-1 text-xs text-gray-400 transition-colors hover:bg-white/10 hover:text-gray-200"
                >
                  {example}
                </button>
              ))}
            </motion.div>
          </motion.div>

          {/* Feature Cards */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.8, duration: 0.6 }}
            className="grid grid-cols-1 gap-4 pb-20 sm:grid-cols-3"
          >
            {[
              {
                icon: "🏢",
                title: "企業検索",
                desc: "企業名・銘柄コード・EDINETコードで上場企業を検索",
              },
              {
                icon: "📈",
                title: "財務サマリ",
                desc: "売上高・純利益・総資産・ROE等の経営指標を時系列で確認",
              },
              {
                icon: "🧩",
                title: "セグメント分析",
                desc: "事業セグメント別の売上・利益・従業員数を可視化",
              },
            ].map((feature, i) => (
              <motion.div
                key={feature.title}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.9 + i * 0.1 }}
                className="glass-card glass-card-hover p-6"
              >
                <span className="mb-3 block text-2xl">{feature.icon}</span>
                <h3 className="mb-1.5 text-sm font-semibold text-gray-200">
                  {feature.title}
                </h3>
                <p className="text-xs text-gray-500 leading-relaxed">
                  {feature.desc}
                </p>
              </motion.div>
            ))}
          </motion.div>
        </main>

        {/* Footer */}
        <footer className="border-t border-white/5 py-6 text-center text-xs text-gray-600">
          Powered by Next.js &amp; FastAPI | Data from EDINET
        </footer>
      </div>
    </div>
  );
}
