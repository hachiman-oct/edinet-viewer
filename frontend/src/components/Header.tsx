"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface HeaderProps {
  apiKey: string;
  onApiKeyChange: (key: string) => void;
}

export default function Header({ apiKey, onApiKeyChange }: HeaderProps) {
  const [showKey, setShowKey] = useState(false);

  return (
    <header className="sticky top-0 z-50 border-b border-white/5 bg-navy-950/80 backdrop-blur-xl">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
        {/* Logo & Title */}
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-indigo-500 to-sky-500 text-lg shadow-lg shadow-indigo-500/20">
            📊
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight gradient-text">
              EDINET XBRL Viewer
            </h1>
            <p className="text-xs text-gray-500">
              Company &amp; Segment Analysis
            </p>
          </div>
        </div>

        {/* API Key Input */}
        <div className="flex items-center gap-3">
          <div className="relative">
            <label
              htmlFor="api-key-input"
              className="absolute -top-5 left-1 text-[10px] font-medium uppercase tracking-widest text-gray-500"
            >
              EDINET API Key
            </label>
            <div className="flex items-center gap-2">
              <input
                id="api-key-input"
                type={showKey ? "text" : "password"}
                value={apiKey}
                onChange={(e) => onApiKeyChange(e.target.value)}
                placeholder="Enter your API key…"
                className="input-field w-64"
              />
              <button
                type="button"
                onClick={() => setShowKey(!showKey)}
                className="rounded-lg border border-white/10 bg-white/5 p-2.5 text-xs text-gray-400 transition-colors hover:bg-white/10 hover:text-gray-200"
                title={showKey ? "Hide key" : "Show key"}
              >
                {showKey ? "🙈" : "👁️"}
              </button>
            </div>
          </div>

          <AnimatePresence>
            {apiKey && (
              <motion.div
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                className="flex h-8 items-center gap-1 rounded-full bg-emerald-400/10 px-3 text-xs font-medium text-emerald-400"
              >
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                Ready
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </header>
  );
}
