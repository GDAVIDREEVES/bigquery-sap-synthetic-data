"""Non-goods supply-chain IC flows: royalty, service, cost-share."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from acdoca_generator.aggregations.segment_pl import pnl_category
from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.supply_chain_templates import (
    INDUSTRY_SUPPLY_CHAINS,
    SupplyChainStep,
)
from acdoca_generator.config.tp_methods import (
    ROLE_TP_METHOD,
    tp_method_for_roles,
)
from acdoca_generator.generators.supply_chain import (
    _AWREF_PREFIX,
    _LEGSPEC_COST_SHARE,
    _LEGSPEC_GOODS,
    _LEGSPEC_ROYALTY,
    _LEGSPEC_SERVICE,
    LegSpec,
    legspec_for,
)


# ---------------------------------------------------------------------------
# Fast (non-spark) tests
# ---------------------------------------------------------------------------


def test_supply_chain_step_default_transaction_type_is_goods() -> None:
    """Backward compat: existing 7-arg literals stay as goods flows."""
    s = SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.0, ("TOLL", "IPPR"))
    assert s.transaction_type == "goods"
    assert s.royalty_rate is None
    assert s.cost_pool_share is None
    assert s.service_basis is None


def test_tp_method_lookup_3_tuple_match() -> None:
    """A 3-tuple hit returns the txn-type-specific method, not the goods method."""
    royalty = tp_method_for_roles("IPPR", "FRMF", "royalty")
    goods = tp_method_for_roles("IPPR", "FRMF", "goods")
    assert royalty.markup_low != goods.markup_low or royalty.markup_high != goods.markup_high
    # Royalty band is positive (rate, not markup); goods band may be negative.
    assert royalty.markup_low >= 0


def test_tp_method_lookup_falls_back_to_goods() -> None:
    """A 3-tuple miss for a non-goods txn_type falls back to the goods entry."""
    # ("TOLL", "IPPR", "service") isn't defined; ("TOLL", "IPPR", "goods") is.
    fallback = tp_method_for_roles("TOLL", "IPPR", "service")
    goods = tp_method_for_roles("TOLL", "IPPR", "goods")
    assert fallback is goods


def test_tp_method_lookup_falls_back_to_default_on_no_match() -> None:
    """Unknown role pair returns the module's _DEFAULT_TP."""
    out = tp_method_for_roles("ZZZ", "QQQ", "goods")
    assert out.code == "TNMM"


def test_legspec_for_each_transaction_type_returns_distinct_gls() -> None:
    """LegSpec routing differs per transaction_type — not a copy/paste bug."""
    goods = legspec_for("goods")
    royalty = legspec_for("royalty")
    service = legspec_for("service")
    cost_share = legspec_for("cost_share")
    # Buyer DR account distinguishes the four
    accts = {goods.buyer_dr_acct, royalty.buyer_dr_acct, service.buyer_dr_acct, cost_share.buyer_dr_acct}
    assert len(accts) == 4
    # Seller CR account: goods + cost_share both hit revenue_ic; royalty + service have their own
    assert royalty.seller_cr_acct == SAMPLE_GL["revenue_royalty"]
    assert service.seller_cr_acct == SAMPLE_GL["revenue_service"]


def test_legspec_unknown_falls_back_to_goods() -> None:
    assert legspec_for("nonsense") is _LEGSPEC_GOODS


def test_legspec_populate_goods_fields_only_for_goods() -> None:
    assert _LEGSPEC_GOODS.populate_goods_fields is True
    assert _LEGSPEC_ROYALTY.populate_goods_fields is False
    assert _LEGSPEC_SERVICE.populate_goods_fields is False
    assert _LEGSPEC_COST_SHARE.populate_goods_fields is False


def test_revenue_royalty_lands_in_other_income_bucket() -> None:
    """The new GL must land in the segment_pl 'other_income' pivot bucket — not 'revenue'."""
    assert pnl_category(SAMPLE_GL["revenue_royalty"]) == "other_income"


def test_royalty_buyer_expense_lands_in_ic_charges() -> None:
    """Royalty buyer's debit (royalty_exp) must reach the segment_pl 'ic_charges' bucket."""
    assert pnl_category(SAMPLE_GL["royalty_exp"]) == "ic_charges"


def test_service_buyer_expense_lands_in_ic_charges() -> None:
    """Service buyer's debit (ic_service_exp) must reach the segment_pl 'ic_charges' bucket."""
    assert pnl_category(SAMPLE_GL["ic_service_exp"]) == "ic_charges"


def test_cost_share_buyer_expense_lands_in_opex() -> None:
    """Cost-share buyer's debit (opex_rd) must land in the segment_pl 'opex' bucket pre-RFAREA-split."""
    assert pnl_category(SAMPLE_GL["opex_rd"]) == "opex"


def test_awref_prefix_distinct_per_transaction_type() -> None:
    prefixes = set(_AWREF_PREFIX.values())
    assert prefixes == {"SC", "RY", "SV", "CS"}


def test_pharma_chains_include_non_goods_templates() -> None:
    """Pharma exercises all four flow types."""
    pharma = INDUSTRY_SUPPLY_CHAINS["pharmaceutical"]
    txn_types = {step.transaction_type for chain in pharma for step in chain}
    assert {"goods", "royalty", "service", "cost_share"}.issubset(txn_types)


# ---------------------------------------------------------------------------
# Spark integration (gated by conftest auto-marker via the `spark` fixture)
# ---------------------------------------------------------------------------


def test_pharma_run_emits_all_four_flow_types(spark) -> None:
    """An end-to-end pharma generation must produce SC + RY + SV + CS AWREF prefixes."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.1,
        include_reversals=False,
        include_closing=False,
        seed=7,
        include_supply_chain=True,
        sc_chains_per_period=10,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    assert res.supply_chain_flows_df is not None
    awrefs = [r.AWREF for r in res.supply_chain_flows_df.select("AWREF").distinct().collect()]
    prefixes = {a[:2] for a in awrefs if a}
    # n_chains=10 cycles through all 5 pharma templates twice → SC + RY + SV + CS all hit
    assert "SC" in prefixes
    assert "RY" in prefixes
    assert "SV" in prefixes
    assert "CS" in prefixes


def test_royalty_ic_doc_balances_to_zero(spark) -> None:
    """Royalty flow produces a 4-line doc whose WSL nets to 0."""
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "CH", "IE", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=20,
        ic_pct=0.0,  # isolate to SC-only
        include_reversals=False,
        include_closing=False,
        seed=7,
        include_supply_chain=True,
        sc_chains_per_period=15,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    royalty_lines = res.acdoca_df.filter(F.col("AWREF").startswith("RY"))
    assert royalty_lines.count() > 0, "expected at least one royalty line"
    # Each BELNR must net to 0 WSL
    by_doc = royalty_lines.groupBy("BELNR").agg(F.sum("WSL").alias("net_wsl")).collect()
    for r in by_doc:
        assert abs(float(r.net_wsl)) < 0.005, f"BELNR {r.BELNR} did not balance: {r.net_wsl}"


def test_royalty_lines_have_no_matnr_or_werks(spark) -> None:
    """Royalty IC lines must blank out MATNR / WERKS / RUNIT (no goods movement)."""
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
        seed=7,
        include_supply_chain=True,
        sc_chains_per_period=15,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    royalty_lines = res.acdoca_df.filter(F.col("AWREF").startswith("RY"))
    assert royalty_lines.count() > 0
    bad = royalty_lines.filter(
        (F.col("MATNR").isNotNull() & (F.col("MATNR") != ""))
        | (F.col("WERKS").isNotNull() & (F.col("WERKS") != ""))
    )
    assert bad.count() == 0, "royalty rows should have empty MATNR and WERKS"


def test_royalty_seller_revenue_lands_in_revenue_royalty_gl(spark) -> None:
    """Seller-side credit on a royalty doc must be GL revenue_royalty (other_income range)."""
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
        seed=7,
        include_supply_chain=True,
        sc_chains_per_period=15,
    )
    res = generate_acdoca_dataframe(spark, cfg)
    royalty_credits = res.acdoca_df.filter(
        F.col("AWREF").startswith("RY") & (F.col("DRCRK") == "H") & (F.col("WSL") < 0)
    )
    racct_values = {r.RACCT for r in royalty_credits.select("RACCT").distinct().collect()}
    # Revenue line uses revenue_royalty; offsetting AP_IC credit is on the buyer-side credit line
    assert SAMPLE_GL["revenue_royalty"] in racct_values
