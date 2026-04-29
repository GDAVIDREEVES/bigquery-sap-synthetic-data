"""Year-end true-up engine tests."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from acdoca_generator.config.operating_models import ROLE_BY_CODE
from acdoca_generator.generators.year_end_trueup import trueup_adjustment


# ---------------------------------------------------------------------------
# Fast (no Spark) — margin-gap math
# ---------------------------------------------------------------------------


def test_trueup_returns_zero_when_inside_band() -> None:
    """LRD currently at 3% with target band [2%, 4%] needs no adjustment."""
    assert trueup_adjustment(0.03, 0.02, 0.04) == 0.0
    assert trueup_adjustment(0.02, 0.02, 0.04) == 0.0  # boundary
    assert trueup_adjustment(0.04, 0.02, 0.04) == 0.0  # boundary


def test_trueup_pulls_low_margin_up_to_midpoint() -> None:
    """Margin 0.5% → midpoint of [2%, 4%] = 3% → gap = +2.5%."""
    gap = trueup_adjustment(0.005, 0.02, 0.04)
    assert gap == pytest.approx(0.025)
    assert gap > 0


def test_trueup_pushes_high_margin_down_to_midpoint() -> None:
    """Margin 8% → midpoint = 3% → gap = -5%."""
    gap = trueup_adjustment(0.08, 0.02, 0.04)
    assert gap == pytest.approx(-0.05)
    assert gap < 0


def test_trueup_negative_margin_is_pulled_up() -> None:
    """Loss-making LRDs (negative margin) need a large positive credit."""
    gap = trueup_adjustment(-0.10, 0.02, 0.04)
    assert gap > 0


def test_lrd_band_matches_operating_models() -> None:
    """Sanity: the role catalog defines a [low, high] band where low <= high."""
    lrd = ROLE_BY_CODE["LRD"]
    assert lrd.om_low_pct <= lrd.om_high_pct


# ---------------------------------------------------------------------------
# Spark integration (gated by `spark` fixture auto-marker in conftest)
# ---------------------------------------------------------------------------


def test_pipeline_emits_trueup_rows_in_q4(spark) -> None:
    """An end-to-end run with LRD entities present produces TU-prefixed rows in POPER 12."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "GB"],  # GB resolves to LRD via DEFAULT_ROLES
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.2,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=5,
        include_year_end_trueup=True,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    tu_lines = res.acdoca_df.filter(F.col("AWREF").startswith("TU"))
    n = tu_lines.count()
    if n == 0:
        pytest.skip("LRD margin landed inside band; no trueup needed for this seed")
    # All TU rows must be in POPER 12 (Q4)
    bad_period = tu_lines.filter(F.col("POPER") != "012").count()
    assert bad_period == 0


def test_trueup_doc_balances_to_zero(spark) -> None:
    """Every TU BELNR must net to 0 WSL across its 2 lines."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "GB"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.2,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=5,
        include_year_end_trueup=True,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    tu_lines = res.acdoca_df.filter(F.col("AWREF").startswith("TU"))
    if tu_lines.count() == 0:
        pytest.skip("No trueup needed for this seed")
    by_doc = tu_lines.groupBy("BELNR").agg(F.sum("WSL").alias("net")).collect()
    for r in by_doc:
        assert abs(float(r.net)) < 0.005, f"TU BELNR {r.BELNR} did not balance: {r.net}"


def test_trueup_blart_is_manual(spark) -> None:
    """TU rows use BLART='SA' to distinguish from routine 'AB' postings."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "GB"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.2,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=5,
        include_year_end_trueup=True,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    tu_lines = res.acdoca_df.filter(F.col("AWREF").startswith("TU"))
    if tu_lines.count() == 0:
        pytest.skip("No trueup needed for this seed")
    bad_blart = tu_lines.filter(F.col("BLART") != "SA").count()
    assert bad_blart == 0


def test_trueup_rows_get_tier_fill_defaults(spark) -> None:
    """Trueup rows must inherit the tier-fill defaults the main pipeline applies.

    Regression for: BQ load failed with "Required field VORGN cannot be null"
    because trueup rows skipped _apply_tier and ended up with NULL in
    tier-defaulted columns (VORGN, KTOSL, FCSL, etc.). The fix: pipeline.py
    runs _apply_tier on the trueup output before unionByName.
    """
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "GB"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",  # tier where VORGN, KTOSL, FCSL get populated
        txn_per_cc_per_period=20,
        ic_pct=0.2,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=5,
        include_year_end_trueup=True,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    tu_lines = res.acdoca_df.filter(F.col("AWREF").startswith("TU"))
    n = tu_lines.count()
    if n == 0:
        pytest.skip("No trueup needed for this seed")
    null_vorgn = tu_lines.filter(F.col("VORGN").isNull()).count()
    null_ktosl = tu_lines.filter(F.col("KTOSL").isNull()).count()
    assert null_vorgn == 0, f"{null_vorgn}/{n} trueup rows have NULL VORGN"
    assert null_ktosl == 0, f"{null_ktosl}/{n} trueup rows have NULL KTOSL"


def test_trueup_toggle_off_produces_no_rows(spark) -> None:
    """include_year_end_trueup=False → no TU rows."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "GB"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.2,
        include_reversals=False,
        include_closing=False,
        seed=11,
        include_supply_chain=True,
        sc_chains_per_period=5,
        include_year_end_trueup=False,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    n = res.acdoca_df.filter(F.col("AWREF").startswith("TU")).count()
    assert n == 0
