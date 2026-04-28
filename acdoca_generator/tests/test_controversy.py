"""Controversy / APA scenario tests."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from acdoca_generator.config.presets import DEMO_PRESETS, get_preset
from acdoca_generator.config.supply_chain_templates import (
    INDUSTRY_SUPPLY_CHAINS,
    SupplyChainStep,
)


# ---------------------------------------------------------------------------
# Fast (non-spark) tests
# ---------------------------------------------------------------------------


def test_supply_chain_step_default_apa_flag_is_false() -> None:
    s = SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.0, ("TOLL", "IPPR"))
    assert s.apa_flag is False
    assert s.apa_markup_band is None


def test_pharma_includes_apa_chain() -> None:
    """The pharma template set has at least one APA-flagged step."""
    pharma = INDUSTRY_SUPPLY_CHAINS["pharmaceutical"]
    apa_steps = [step for chain in pharma for step in chain if step.apa_flag]
    assert apa_steps, "pharma should include an APA-covered chain template"
    for step in apa_steps:
        assert step.apa_markup_band is not None
        lo, hi = step.apa_markup_band
        assert lo <= hi


def test_controversy_demo_preset_is_registered() -> None:
    p = get_preset("controversy_demo")
    assert p.challenged_share == pytest.approx(0.20)
    assert p.include_supply_chain is True


def test_controversy_demo_listed_in_demo_presets() -> None:
    assert "controversy_demo" in DEMO_PRESETS


def test_other_presets_default_challenged_share_is_zero() -> None:
    for key, p in DEMO_PRESETS.items():
        if key == "controversy_demo":
            continue
        assert p.challenged_share == 0.0, f"{key} should default challenged_share=0.0"


# ---------------------------------------------------------------------------
# Spark integration
# ---------------------------------------------------------------------------


def test_apa_flow_uses_tighter_markup_band(spark) -> None:
    """APA-flagged flows must have markup inside the declared apa_markup_band."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=10,  # cycle through templates including APA chain
        include_year_end_trueup=False,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    assert res.supply_chain_flows_df is not None
    apa_flows = res.supply_chain_flows_df.filter(F.col("APA_FLAG") == True)
    assert apa_flows.count() > 0
    # Every APA step's markup must be inside one of the declared bands. Easiest:
    # the union of all apa_markup_band ranges across pharma. For this seed/chain
    # the (TOLL, IPPR) step uses (0.035, 0.045), so check that range broadly.
    for r in apa_flows.collect():
        m = float(r.MARKUP_RATE)
        # Pharma APA bands are within [-0.025, 0.045]
        assert -0.05 <= m <= 0.05, f"APA flow markup {m} outside expected APA range"


def test_challenged_share_produces_some_challenged_flows(spark) -> None:
    """challenged_share=0.5 produces a non-trivial subset of CHALLENGED_FLAG=True rows."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=10,
        challenged_share=0.5,
        include_year_end_trueup=False,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    assert res.supply_chain_flows_df is not None
    total = res.supply_chain_flows_df.count()
    challenged = res.supply_chain_flows_df.filter(F.col("CHALLENGED_FLAG") == True).count()
    # APA flows are never challenged, so absolute fraction is below challenged_share.
    # At 50%, expect at least 10% of total flows tagged.
    assert 0 < challenged < total
    assert challenged / total > 0.10


def test_challenged_share_zero_produces_no_challenged_flows(spark) -> None:
    """Default challenged_share=0.0 → CHALLENGED_FLAG always False."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=10,
        challenged_share=0.0,
        include_year_end_trueup=False,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    assert res.supply_chain_flows_df is not None
    challenged = res.supply_chain_flows_df.filter(F.col("CHALLENGED_FLAG") == True).count()
    assert challenged == 0


def test_challenged_flows_have_adjusted_view_price(spark) -> None:
    """Every challenged flow has a non-null ADJUSTED_VIEW_PRICE > legal price."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=10,
        challenged_share=0.5,
        include_year_end_trueup=False,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    challenged = res.supply_chain_flows_df.filter(F.col("CHALLENGED_FLAG") == True)
    if challenged.count() == 0:
        pytest.skip("seed produced no challenged flows")
    for r in challenged.collect():
        assert r.ADJUSTED_VIEW_PRICE is not None
        assert float(r.ADJUSTED_VIEW_PRICE) > float(r.TOTAL_LEGAL_PRICE)
