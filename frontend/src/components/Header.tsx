"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

export default function Header() {
  const pathname = usePathname();

  const isHome = pathname === "/";

  return (
    <header className="sticky top-0 z-50 border-b border-white/5 bg-navy-950/80 backdrop-blur-xl">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
        {/* Logo & Title */}
        <Link href="/" className="flex items-center gap-3 transition-opacity hover:opacity-80">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-indigo-500 to-sky-500 text-lg shadow-lg shadow-indigo-500/20">
            📊
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight gradient-text">
              EDINET XBRL Viewer
            </h1>
            <p className="text-xs text-gray-500">
              企業分析 &amp; セグメント可視化
            </p>
          </div>
        </Link>

        {/* Navigation */}
        <nav className="flex items-center gap-4">
          {!isHome && (
            <Link
              href="/"
              className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-gray-400 transition-colors hover:bg-white/5 hover:text-gray-200"
            >
              🏠 ホーム
            </Link>
          )}
          {/* Status badge */}
          <div className="flex h-8 items-center gap-1 rounded-full bg-emerald-400/10 px-3 text-xs font-medium text-emerald-400">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
            Connected
          </div>
        </nav>
      </div>
    </header>
  );
}
