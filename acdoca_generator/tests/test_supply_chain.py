"""Financial supply chain generation and Dash graph helpers."""

from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.config.supply_chain_templates import (
    INDUSTRY_SUPPLY_CHAINS,
    SupplyChainStep,
    supply_chain_templates_for_industry,
)
from acdoca_generator.config.tp_methods import ROLE_TP_METHOD
from acdoca_generator.dash_app.graph import build_cytoscape_elements, filter_flow_records
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.generators.supply_chain import (
    _chain_start_poper,
    _flow_json_default,
    _pick_markup,
    _poper_for_step,
)


@pytest.mark.skipif(
    os.environ.get("ACDOCA_RUN_SPARK_TESTS") != "1",
    reason="Slow Spark test; run with ACDOCA_RUN_SPARK_TESTS=1 (requires Java)",
)
def test_supply_chain_generates_hops_and_sc_awref() -> None:
    spark = SparkSession.builder.master("local[2]").appName("test_sc").getOrCreate()
    try:
        cfg = GenerationConfig(
            industry_key="pharmaceutical",
            country_isos=["US", "DE", "CH", "IE", "IN"],
            fiscal_year=2026,
            fiscal_variant="calendar",
            complexity="medium",
            txn_per_cc_per_period=50,
            include_reversals=False,
            include_closing=False,
            seed=7,
            ic_pct=0.1,
            include_supply_chain=True,
            sc_chains_per_period=2,
        )
        res = generate_acdoca_dataframe(spark, cfg)
        assert res.supply_chain_flows_df is not None
        assert res.supply_chain_flows_df.count() > 0
        sc_lines = res.acdoca_df.filter(F.col("AWREF").startswith("SC"))
        assert sc_lines.count() >= 4
        awrefs = [r.AWREF for r in res.supply_chain_flows_df.select("AWREF").distinct().collect()]
        assert all(str(a).startswith("SC") for a in awrefs)
    finally:
        spark.stop()


def test_total_legal_price_formula() -> None:
    std = Decimal("150")
    mup = Decimal("0.05")
    vol = Decimal("100")
    legal = (std * (Decimal("1") + mup) * vol).quantize(Decimal("0.01"))
    assert legal == Decimal("15750.00")


def test_chain_start_poper_in_range() -> None:
    for seed in (1, 7, 42, 99, 12345):
        for chain_i in range(50):
            p = _chain_start_poper(seed, chain_i)
            assert 1 <= p <= 12


def test_poper_for_step_causal_ordering() -> None:
    """Within a chain, step POPERs are non-decreasing (clamped at 12)."""
    for seed in (1, 7, 42, 99):
        for chain_i in range(50):
            popers = [_poper_for_step(seed, chain_i, s) for s in range(5)]
            assert popers == sorted(popers), f"seed={seed} chain={chain_i}: {popers}"
            assert all(1 <= p <= 12 for p in popers)


def test_supply_chain_template_count_per_industry() -> None:
    """Each industry has at least 2 templates, so n_chains >= 2 exercises diversity."""
    for industry in INDUSTRY_SUPPLY_CHAINS:
        templates = supply_chain_templates_for_industry(industry)
        assert len(templates) >= 2, f"{industry} has only {len(templates)} template(s)"


def test_template_role_pairs_use_known_tp_methods_or_default() -> None:
    """Every step's tp_method_key either matches ROLE_TP_METHOD or matches its (source, dest) pair."""
    for industry, chains in INDUSTRY_SUPPLY_CHAINS.items():
        for chain in chains:
            for step in chain:
                assert step.tp_method_key == (step.source_role, step.dest_role), (
                    f"{industry}: step {step.step_number} tp_method_key {step.tp_method_key} "
                    f"diverges from ({step.source_role}, {step.dest_role}); intentional override "
                    f"is allowed but must be reviewed."
                )


def test_pick_markup_in_range() -> None:
    """Sampled markups stay inside [lo, hi] for every (role pair, transaction type)."""
    for (src, dst, txn_type), tp in ROLE_TP_METHOD.items():
        for chain_i in range(20):
            for step_i in range(5):
                for buyer_i in range(5):
                    m = _pick_markup(7, chain_i, step_i, buyer_i, tp.markup_low, tp.markup_high)
                    assert tp.markup_low <= m <= tp.markup_high, (
                        f"{src}->{dst} ({txn_type}): markup {m} outside [{tp.markup_low}, {tp.markup_high}]"
                    )


def test_pick_markup_distribution_centered() -> None:
    """Triangular distribution mean should be near the midpoint, not the lo/hi edge."""
    lo, hi = 0.03, 0.05
    samples = [
        _pick_markup(11, c, s, b, lo, hi)
        for c in range(40)
        for s in range(5)
        for b in range(5)
    ]
    mean = sum(samples) / len(samples)
    midpoint = (lo + hi) / 2
    width = hi - lo
    # Triangular(0,1) mean is 0.5; allow ±15% of width for hash-based PRNG noise
    assert abs(mean - midpoint) < 0.15 * width, (
        f"mean {mean} drifted from midpoint {midpoint} by more than 15% of width {width}"
    )


def test_step_tp_method_key_resolves_via_role_table() -> None:
    """Every step resolves to a TP method via the (source, dest, txn_type) lookup."""
    from acdoca_generator.config.tp_methods import tp_method_for_roles

    for industry, chains in INDUSTRY_SUPPLY_CHAINS.items():
        for chain in chains:
            for step in chain:
                tp = tp_method_for_roles(step.source_role, step.dest_role, step.transaction_type)
                assert tp is not None, f"{industry}: step {step.step_number} resolved to None"
                assert tp.markup_low <= tp.markup_high, (
                    f"{industry}: step {step.step_number} band inverted: {tp.markup_low} > {tp.markup_high}"
                )


def test_export_helper_serializes_decimal_and_dates(tmp_path: Path) -> None:
    """The Decimal/date encoder used by export_supply_chain_flows_json round-trips."""
    from datetime import date

    sample = {"price": Decimal("123.45"), "as_of": date(2026, 4, 27), "qty": 7}
    out = tmp_path / "sample.json"
    out.write_text(json.dumps(sample, default=_flow_json_default))
    loaded = json.loads(out.read_text())
    assert loaded == {"price": 123.45, "as_of": "2026-04-27", "qty": 7}


def test_supply_chain_step_default_fanout_all_is_false() -> None:
    """Backward-compat: existing templates that don't pass fanout_all default to False."""
    s = SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.0, ("TOLL", "IPPR"))
    assert s.fanout_all is False


def test_cytoscape_elements_from_records() -> None:
    recs = [
        {
            "CHAIN_ID": "SC000001",
            "STEP_NUMBER": 1,
            "MATERIAL_TYPE": "RAW",
            "MATNR": "API-001",
            "IP_OWNER": "3000",
            "SELLING_COMPANY": "1000",
            "BUYING_COMPANY": "2000",
            "RESIDUAL_PROFIT_OWNER": "3000",
            "WERKS": "P1000",
            "TP_METHOD": "CUP",
            "AWREF": "SC000000000",
            "SELLER_ROLE": "TOLL",
            "BUYER_ROLE": "IPPR",
            "SELLER_LAND1": "IN",
            "BUYER_LAND1": "US",
            "TOTAL_VOLUME": 10.0,
            "TOTAL_LEGAL_PRICE": 100.0,
        }
    ]
    filtered = filter_flow_records(recs)
    elements, node_map, edge_map = build_cytoscape_elements(filtered)
    nodes = [e for e in elements if "source" not in e["data"]]
    edges = [e for e in elements if "source" in e["data"]]
    assert len(nodes) == 2
    assert len(edges) == 1
    assert "n_1000" in node_map
    assert len(edge_map) == 1
