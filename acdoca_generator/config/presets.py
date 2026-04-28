"""Named demo configurations for repeatable client workshops (velocity + consistency)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class DemoPreset:
    key: str
    label: str
    industry_key: str
    country_isos_csv: str
    fiscal_year: int
    fiscal_variant: str  # calendar | april
    complexity: str
    txn_per_cc_per_period: int
    ic_pct: Optional[float]  # None → industry ic_share_default at generation time
    include_reversals: bool
    include_closing: bool
    validation_profile: str  # strict | fast
    include_supply_chain: bool = True
    sc_chains_per_period: int = 50
    include_segment_pl: bool = False
    challenged_share: float = 0.0


DEMO_PRESETS: Dict[str, DemoPreset] = {
    "quick_smoke": DemoPreset(
        key="quick_smoke",
        label="Quick smoke (~2–5 min)",
        industry_key="consumer_goods",
        country_isos_csv="US,DE",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=200,
        ic_pct=None,
        include_reversals=True,
        include_closing=False,
        validation_profile="fast",
        include_supply_chain=False,
    ),
    "tp_workshop": DemoPreset(
        key="tp_workshop",
        label="Transfer pricing workshop",
        industry_key="pharmaceutical",
        country_isos_csv="US,DE,CH,IE",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=1500,
        ic_pct=0.35,
        include_reversals=True,
        include_closing=True,
        validation_profile="strict",
    ),
    "globe_lite": DemoPreset(
        key="globe_lite",
        label="GloBE / multi-country lite",
        industry_key="media",
        country_isos_csv="US,GB,FR,IN",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=600,
        ic_pct=None,
        include_reversals=True,
        include_closing=True,
        validation_profile="fast",
    ),
    "ml_features": DemoPreset(
        key="ml_features",
        label="ML / wide schema features",
        industry_key="technology",
        country_isos_csv="US,DE,GB,IN",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="high",
        txn_per_cc_per_period=2000,
        ic_pct=0.30,
        include_reversals=True,
        include_closing=True,
        validation_profile="strict",
    ),
    "supply_chain_demo": DemoPreset(
        key="supply_chain_demo",
        label="Financial supply chain demo",
        industry_key="pharmaceutical",
        country_isos_csv="US,DE,CH,IE,IN,FR,GB",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=1000,
        ic_pct=0.35,
        include_reversals=True,
        include_closing=True,
        validation_profile="strict",
        include_supply_chain=True,
        sc_chains_per_period=50,
    ),
    "controversy_demo": DemoPreset(
        key="controversy_demo",
        label="Controversy / APA scenarios",
        industry_key="pharmaceutical",
        country_isos_csv="US,DE,CH,IE,IN,GB",
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=500,
        ic_pct=0.30,
        include_reversals=True,
        include_closing=True,
        validation_profile="strict",
        include_supply_chain=True,
        sc_chains_per_period=40,
        include_segment_pl=True,
        challenged_share=0.20,  # 20% of flows tagged as challenged by tax authority
    ),
}


def preset_keys() -> list[str]:
    return list(DEMO_PRESETS.keys())


def get_preset(key: str) -> DemoPreset:
    if key not in DEMO_PRESETS:
        raise KeyError(key)
    return DEMO_PRESETS[key]
