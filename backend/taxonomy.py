"""
EDINET タクソノミに基づく XBRL タグ定義
=========================================

タグ名は EDINET のタクソノミ要素を参照して定義する。
https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W1E63010CXW1E6A010DSPSch&uji.bean=ee.bean.W1E63010.EEW1E63010BLogicBean&TID=2&PID=W1E63010&lgKbn=2&dflg=0&iflg=0&preId=1

各リストは **優先度の高い順** に記載する。
`extract_xbrl_data` はリストの先頭から順にマッチを試行し、
最初にヒットした値を採用する。

新しいタグを追加する場合は、該当リストに追記するだけでよい。
"""

# ---------------------------------------------------------------------------
# Company Summary — 企業サマリー指標
# ---------------------------------------------------------------------------
# キー名は main.py 側の company_summary dict のキーと対応している。
# "PER" と "ROE" は PBR の算出に使用するため、個別のキーとして定義する。

COMPANY_TAGS: dict[str, list[str]] = {
    "Accounting Standards": [
        "AccountingStandardsDEI",
    ],
    "Net Sales": [
        # 売上高、経営指標等
        "NetSalesSummaryOfBusinessResults",
        # 売上収益（IFRS）、経営指標等
        "RevenueIFRSSummaryOfBusinessResults",
        # 売上収益（JMIS）、経営指標等
        "RevenueJMISSummaryOfBusinessResults",
        # 売上高（US GAAP）、経営指標等
        "RevenuesUSGAAPSummaryOfBusinessResults",

        # 営業収益、経営指標等
        "OperatingRevenue1SummaryOfBusinessResults",
        # 営業収入、経営指標等
        "OperatingRevenue1SummaryOfBusinessResults",
        # 営業総収入、経営指標等
        "GrossOperatingRevenueSummaryOfBusinessResults",
        # 経常収益、経営指標等
        "OrdinaryIncomeSummaryOfBusinessResults",
        # 正味収入保険料、経営指標等、保険業
        "NetPremiumsWrittenSummaryOfBusinessResultsINS",
    ],
    "Net Income": [
        # 当期純利益又は当期純損失（△）、経営指標等
        "NetIncomeLossSummaryOfBusinessResults",
        # 当期利益又は当期損失（△）（IFRS）、経営指標等
        "ProfitLossIFRSSummaryOfBusinessResults",
        # 当期利益又は当期損失（△）（JMIS）、経営指標等
        "ProfitLossJMISSummaryOfBusinessResults",
        # 当社株主に帰属する純利益又は純損失（△）（US GAAP）、経営指標等
        "NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults",

        # 親会社株主に帰属する当期純利益又は親会社株主に帰属する当期純損失（△）、経営指標等
        "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults",
        # 当期利益又は当期損失（△）：親会社の所有者に帰属（IFRS）、経営指標等
        "ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults",
        # 当期利益又は当期損失（△）：親会社の所有者に帰属（JMIS）、経営指標等
        "ProfitLossAttributableToOwnersOfParentJMISSummaryOfBusinessResults",
    ],
    "Total Assets": [
        # 総資産額、経営指標等
        "TotalAssetsSummaryOfBusinessResults",
        # 総資産額（IFRS）、経営指標等
        "TotalAssetsIFRSSummaryOfBusinessResults",
        # 総資産額（JMIS）、経営指標等
        "TotalAssetsJMISSummaryOfBusinessResults",
        # 総資産額（US GAAP）、経営指標等
        "TotalAssetsUSGAAPSummaryOfBusinessResults",
    ],
    "Net Assets": [
        # 純資産額、経営指標等
        "NetAssetsSummaryOfBusinessResults",
        # 株主資本（IFRS）、経営指標等
        "EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults",
        # 株主資本合計（IFRS）、経営指標等
        "EquityIFRSSummaryOfBusinessResults",
        # 純資産額（JMIS）、経営指標等
        "NetAssetsJMISSummaryOfBusinessResults",
        # 純資産額（US GAAP）、経営指標等
        "NetAssetsUSGAAPSummaryOfBusinessResults",
        # 株主資本合計（US GAAP）、経営指標等
        "EquityUSGAAPSummaryOfBusinessResults",
    ],
    "PER": [
        # 株価収益率、経営指標等
        "PriceEarningsRatioSummaryOfBusinessResults",
        # 株価収益率（IFRS）、経営指標等
        "PriceEarningsRatioIFRSSummaryOfBusinessResults",
        # 株価収益率（JMIS）、経営指標等
        "PriceEarningsRatioJMISSummaryOfBusinessResults",
        # 株価収益率（US GAAP）、経営指標等
        "PriceEarningsRatioUSGAAPSummaryOfBusinessResults",
    ],
    "ROE": [
        # 自己資本利益率、経営指標等
        "RateOfReturnOnEquitySummaryOfBusinessResults",
        # 親会社所有者帰属持分利益率、経営指標等
        "RateOfReturnOnEquityIFRSSummaryOfBusinessResults",
        # 親会社所有者帰属持分利益率（JMIS）、経営指標等
        "RateOfReturnOnEquityJMISSummaryOfBusinessResults",
        # 株主資本利益率（US GAAP）、経営指標等
        "RateOfReturnOnEquityUSGAAPSummaryOfBusinessResults",
    ],
}


# ---------------------------------------------------------------------------
# Segment Details — セグメント情報
# ---------------------------------------------------------------------------
# context は Duration (期間) と Instant (時点) の 2 種類がある。
# "context" フィールドで使い分ける:
#   "duration" → CurrentYearDuration_<segment>
#   "instant"  → CurrentYearInstant_<segment>

SEGMENT_TAGS: dict[str, dict] = {
    # US GAAPはタクソノミが登録されていないためテキストブロックで開示されている．したがって取得は困難
    "Sales to External Customers (外部顧客への売上高)": {
        "context": "duration",
        "tags": [
            # 外部顧客への売上高
            "RevenuesFromExternalCustomers",
            # 外部顧客への売上収益（IFRS）
            "RevenueFromExternalCustomersIFRS",
            # 外部顧客への売上高（IFRS）
            "SalesToExternalCustomersIFRS",

            # 銀行・保険業
            # "OrdinaryIncomeBySegment",
            # 粗利益: MUFG
            "NetRevenue",
        ],
    },
    "Segment Profit (セグメント利益)": {
        "context": "duration",
        "tags": [
            # セグメント利益又は損失（△） [タイトル項目]
            "SegmentProfitLossAbstrac",
            # セグメント利益（△損失）（IFRS）
            "SegmentProfitLossIFRS",

            # 営業純益: MUFG
            "OperatingProfit",
        ],
    },
    "Employees (連結従業員数)": {
        "context": "instant",
        "tags": [
            # 従業員数
            "NumberOfEmployees",
        ],
    },
}
