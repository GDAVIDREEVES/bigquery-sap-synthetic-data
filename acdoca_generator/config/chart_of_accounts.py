"""Universal chart of accounts skeleton (SPEC §5.2)."""

from __future__ import annotations

# (low, high, category)
ACCOUNT_RANGES: tuple[tuple[int, int, str], ...] = (
    (1_000_000, 1_999_999, "cash_banks"),
    (10_000_000, 19_999_999, "receivables"),
    (20_000_000, 29_999_999, "inventory"),
    (30_000_000, 39_999_999, "fixed_assets"),
    (40_000_000, 49_999_999, "other_assets"),
    (50_000_000, 59_999_999, "payables"),
    (60_000_000, 69_999_999, "other_liabilities"),
    (70_000_000, 79_999_999, "equity"),
    (80_000_000, 84_999_999, "revenue"),
    (85_000_000, 89_999_999, "other_income"),
    (90_000_000, 91_999_999, "cogs"),
    (92_000_000, 94_999_999, "opex"),
    (95_000_000, 96_999_999, "ic_charges"),
    (97_000_000, 98_999_999, "depreciation"),
    (99_000_000, 99_999_999, "tax_extraordinary"),
)

# Representative GL accounts used by generators (string, SAP-style leading zeros optional)
SAMPLE_GL: dict[str, str] = {
    "cash": "0010000",
    "ar_trade": "0100000",
    "ar_ic": "0110000",
    "inventory_rm": "0200000",
    "inventory_fg": "0220000",
    "ap_trade": "0500000",
    "ap_ic": "0510000",
    "revenue_tp": "0800000",
    "revenue_service": "0810000",
    "revenue_ic": "0820000",
    "cogs": "0900000",
    "cogs_ic": "0910000",
    "opex_rd": "0925000",
    "opex_sga": "0930000",
    "mgmt_fee_exp": "0950000",
    "royalty_exp": "0955000",
    "ic_service_exp": "0960000",
    "interest_exp": "0600000",
    "interest_inc": "0860000",
    "retained_earnings": "0710000",
}
