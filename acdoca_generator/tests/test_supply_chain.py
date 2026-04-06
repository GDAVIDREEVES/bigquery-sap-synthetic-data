"""Financial supply chain generation and Dash graph helpers."""

from __future__ import annotations

import os
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.dash_app.graph import build_cytoscape_elements, filter_flow_records
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe


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
