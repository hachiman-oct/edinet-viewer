"use client";

import { useState, useCallback } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";

export default function Header() {
  const pathname = usePathname();
  const router = useRouter();
  const [quickQuery, setQuickQuery] = useState("");

  const isHome = pathname === "/";

  const handleQuickSearch = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      const trimmed = quickQuery.trim();
      if (!trimmed) return;
      router.push(`/search?q=${encodeURIComponent(trimmed)}`);
      setQuickQuery("");
    },
    [quickQuery, router]
  );

  return (
    <header className="sticky top-0 z-50 border-b border-white/5 bg-navy-950/80 backdrop-blur-xl">
      <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-6 py-4">
        {/* Logo & Title */}
        <Link
          href="/"
          className="flex shrink-0 items-center gap-3 transition-opacity hover:opacity-80"
        >
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-indigo-500 to-sky-500 text-lg shadow-lg shadow-indigo-500/20">
            📊
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight gradient-text">
              EDINET XBRL Viewer
            </h1>
            <p className="hidden text-xs text-gray-500 sm:block">
              企業分析 &amp; セグメント可視化
            </p>
          </div>
        </Link>

        {/* Navigation & Quick Search */}
        <div className="flex flex-1 items-center justify-end gap-3">
          {!isHome && (
            <form
              onSubmit={handleQuickSearch}
              className="flex w-full max-w-xs items-center rounded-xl border border-white/10 bg-white/5 px-3 py-1.5 transition-all focus-within:border-indigo-500/50 focus-within:bg-white/8 focus-within:ring-2 focus-within:ring-indigo-500/20"
            >
              <span className="mr-2 text-xs text-gray-500">🔍</span>
              <input
                type="text"
                value={quickQuery}
                onChange={(e) => setQuickQuery(e.target.value)}
                placeholder="企業名・銘柄コードで検索..."
                autoComplete="off"
                data-1p-ignore
                className="w-full bg-transparent text-xs text-gray-100 placeholder-gray-500 outline-none"
              />
              {quickQuery.trim() && (
                <button
                  type="submit"
                  className="ml-1 shrink-0 rounded-md bg-indigo-500/20 px-2 py-0.5 text-[11px] font-medium text-indigo-300 transition-colors hover:bg-indigo-500/30"
                >
                  検索
                </button>
              )}
            </form>
          )}

          {!isHome && (
            <Link
              href="/"
              className="flex shrink-0 items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-gray-400 transition-colors hover:bg-white/5 hover:text-gray-200"
            >
              <span>🏠</span>
              <span className="hidden sm:inline">ホーム</span>
            </Link>
          )}
        </div>
      </div>
    </header>
  );
}
