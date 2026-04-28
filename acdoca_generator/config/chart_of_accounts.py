"""Universal chart of accounts skeleton (SPEC §5.2)."""

from __future__ import annotations

# (low, high, category) — ranges align with the 6-digit SAMPLE_GL convention used by
# the generators (e.g. revenue_tp = "0800000" → 800000 ∈ [800_000, 849_999] = revenue).
ACCOUNT_RANGES: tuple[tuple[int, int, str], ...] = (
    (10_000, 19_999, "cash_banks"),
    (100_000, 199_999, "receivables"),
    (200_000, 299_999, "inventory"),
    (300_000, 399_999, "fixed_assets"),
    (400_000, 499_999, "other_assets"),
    (500_000, 599_999, "payables"),
    (600_000, 699_999, "other_liabilities"),
    (700_000, 799_999, "equity"),
    (800_000, 849_999, "revenue"),
    (850_000, 899_999, "other_income"),
    (900_000, 919_999, "cogs"),
    (920_000, 949_999, "opex"),
    (950_000, 969_999, "ic_charges"),
    (970_000, 989_999, "depreciation"),
    (990_000, 999_999, "tax_extraordinary"),
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
    "revenue_royalty": "0855000",  # other_income range (850k–899k); royalty income at IP-owning principal
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
