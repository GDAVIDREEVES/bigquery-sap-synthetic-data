"""Industry templates (SPEC §5)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IndustryTemplate:
    key: str
    label: str
    gross_margin_low: float
    gross_margin_high: float
    transaction_mix: dict[str, float]
    seasonality_month: dict[int, float]  # 1-12 -> weight
    ic_share_default: float = 0.25

    def normalized_mix(self) -> dict[str, float]:
        s = sum(self.transaction_mix.values())
        return {k: v / s for k, v in self.transaction_mix.items()}


def _flat_seasonality() -> dict[int, float]:
    return {m: 1.0 for m in range(1, 13)}


def _q4_peak() -> dict[int, float]:
    w = {m: 1.0 for m in range(1, 13)}
    w[10] = w[11] = w[12] = 1.25
    return w


PHARMA_MIX = {
    "third_party_revenue": 0.25,
    "procurement_cogs": 0.20,
    "ic_goods": 0.15,
    "ic_services": 0.10,
    "payroll": 0.12,
    "rd": 0.08,
    "depreciation": 0.03,
    "tax": 0.03,
    "other": 0.04,
}

INDUSTRIES: dict[str, IndustryTemplate] = {
    "pharmaceutical": IndustryTemplate(
        "pharmaceutical",
        "Pharmaceutical",
        0.65,
        0.75,
        PHARMA_MIX,
        _flat_seasonality(),
    ),
    "medical_device": IndustryTemplate(
        "medical_device",
        "Medical Device",
        0.55,
        0.65,
        {**PHARMA_MIX, "ic_goods": 0.18, "procurement_cogs": 0.17},
        _flat_seasonality(),
    ),
    "consumer_goods": IndustryTemplate(
        "consumer_goods",
        "Consumer Goods",
        0.40,
        0.50,
        {
            "third_party_revenue": 0.35,
            "procurement_cogs": 0.22,
            "ic_goods": 0.08,
            "ic_services": 0.08,
            "payroll": 0.10,
            "rd": 0.03,
            "depreciation": 0.04,
            "tax": 0.04,
            "other": 0.06,
        },
        _q4_peak(),
        ic_share_default=0.18,
    ),
    "technology": IndustryTemplate(
        "technology",
        "Technology",
        0.60,
        0.70,
        {
            "third_party_revenue": 0.20,
            "procurement_cogs": 0.12,
            "ic_goods": 0.10,
            "ic_services": 0.18,
            "payroll": 0.15,
            "rd": 0.18,
            "depreciation": 0.02,
            "tax": 0.03,
            "other": 0.02,
        },
        _flat_seasonality(),
        ic_share_default=0.35,
    ),
    "media": IndustryTemplate(
        "media",
        "Media",
        0.45,
        0.55,
        {
            "third_party_revenue": 0.28,
            "procurement_cogs": 0.10,
            "ic_goods": 0.05,
            "ic_services": 0.20,
            "payroll": 0.12,
            "rd": 0.05,
            "depreciation": 0.08,
            "tax": 0.04,
            "other": 0.08,
        },
        _flat_seasonality(),
        ic_share_default=0.28,
    ),
}

# Backward-compatible names (docs/jobs may still pass legacy keys).
INDUSTRY_ALIASES: dict[str, str] = {
    "consumer_products": "consumer_goods",
}


def canonical_industry_key(key: str) -> str:
    k = (key or "").strip()
    return INDUSTRY_ALIASES.get(k, k)


def get_industry(key: str) -> IndustryTemplate:
    ck = canonical_industry_key(key)
    if ck not in INDUSTRIES:
        raise KeyError(key)
    return INDUSTRIES[ck]


def industry_keys() -> list[str]:
    return list(INDUSTRIES.keys())
