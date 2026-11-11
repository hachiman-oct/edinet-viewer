/**
 * 金額（円）を「1,234百万円」形式にフォーマットする
 */
export function formatMillionYen(
  val: number | null | undefined,
  fallback = "—"
): string {
  if (val === null || val === undefined) return fallback;
  if (val === 0) return "0百万円";

  const millions = val / 1_000_000;
  if (Math.abs(millions) < 1) {
    return `${millions.toLocaleString("ja-JP", {
      minimumFractionDigits: 1,
      maximumFractionDigits: 2,
    })}百万円`;
  }
  return `${Math.round(millions).toLocaleString("ja-JP")}百万円`;
}

/**
 * グラフの軸ラベル用に百万円単位の数値文字列（カンマ区切り、単位なし）を返す
 */
export function formatMillionNumber(val: number): string {
  return Math.round(val / 1_000_000).toLocaleString("ja-JP");
}

/**
 * 小数（0.1234など）をパーセント表記（12.34%）にフォーマットする
 */
export function formatPercent(
  val: number | null | undefined,
  fallback = "—"
): string {
  if (val === null || val === undefined) return fallback;
  return `${(val * 100).toFixed(2)}%`;
}

/**
 * 倍率・指標（PER/PBRなど）を小数2桁にフォーマットする
 */
export function formatRatio(
  val: number | null | undefined,
  fallback = "—"
): string {
  if (val === null || val === undefined) return fallback;
  return val.toFixed(2);
}

/**
 * 人数などの整数をカンマ区切りにフォーマットする
 */
export function formatNumber(
  val: number | null | undefined,
  suffix = "",
  fallback = "—"
): string {
  if (val === null || val === undefined) return fallback;
  return `${val.toLocaleString("ja-JP")}${suffix}`;
}

