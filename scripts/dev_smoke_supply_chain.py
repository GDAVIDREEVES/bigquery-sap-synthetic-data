"""Throwaway dev smoke test for the supply chain viewer pipeline.

Run from repo root:
    .venv-plan/bin/python scripts/dev_smoke_supply_chain.py

Generates a small ACDOCA dataset with supply chain tracing enabled, asserts
intercompany docs balance to zero, and writes flows JSON for the Dash viewer.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make Spark workers use the same Python as the driver (avoids 3.9-vs-3.12 mismatch)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Tame Spark logs and JVM heap for local smoke runs
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.generators.pipeline import (
    GenerationConfig,
    export_supply_chain_json,
    generate_acdoca_dataframe,
)


def main() -> int:
    spark = (
        SparkSession.builder.appName("sc-smoke")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE", "IE", "CH", "GB"],  # +GB so an LRD lands in the run for trueup
        fiscal_year=2025,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=2,
        include_reversals=False,
        include_closing=False,
        seed=42,
        group_currency="USD",
        ic_pct=0.30,
        include_supply_chain=True,
        sc_chains_per_period=12,
        include_segment_pl=True,
        include_year_end_trueup=True,
        challenged_share=0.20,  # exercise controversy tagging
    )

    print(f"[smoke] generating with {len(cfg.country_isos)} countries, "
          f"sc_chains_per_period={cfg.sc_chains_per_period} ...")
    result = generate_acdoca_dataframe(spark, cfg)

    if result.supply_chain_flows_df is None:
        print("[smoke] FAIL: include_supply_chain=True but no flows produced")
        return 2

    flows = result.supply_chain_flows_df.cache()
    acdoca = result.acdoca_df.cache()

    n_flows = flows.count()
    n_acdoca = acdoca.count()
    print(f"[smoke] flow rows:   {n_flows}")
    print(f"[smoke] acdoca rows: {n_acdoca}")

    if n_flows == 0:
        print("[smoke] FAIL: zero flow rows")
        return 2

    print("[smoke] flow sample:")
    flows.select(
        "CHAIN_ID", "STEP_NUMBER", "SELLER_ROLE", "BUYER_ROLE",
        "SELLER_LAND1", "BUYER_LAND1", "TP_METHOD", "MARKUP_RATE",
        "TOTAL_LEGAL_PRICE",
    ).show(8, truncate=False)

    # IC balance check: every IC document (RASSC populated) must net to 0
    ic_only = acdoca.filter(F.col("RASSC").isNotNull() & (F.col("RASSC") != ""))
    by_doc = (
        ic_only.groupBy("BELNR")
        .agg(F.sum("WSL").alias("net_wsl"))
        .filter(F.abs(F.col("net_wsl")) > 0.01)
    )
    n_bad = by_doc.count()
    if n_bad:
        print(f"[smoke] FAIL: {n_bad} IC documents are out of balance")
        by_doc.show(10, truncate=False)
        return 3
    print("[smoke] IC balance: all IC docs net to 0 ✓")

    # Cross-border share — operational TP relevance
    cross_border = flows.filter(F.col("SELLER_LAND1") != F.col("BUYER_LAND1")).count()
    print(f"[smoke] cross-border flow share: {cross_border}/{n_flows} "
          f"({(cross_border / n_flows * 100):.1f}%)")

    # POPER spread + causal ordering within each chain
    print("[smoke] POPER spread:")
    flows.groupBy("POPER").count().orderBy("POPER").show(truncate=False)
    chain_step_periods = (
        flows.groupBy("CHAIN_ID")
        .agg(F.collect_list(F.struct("STEP_NUMBER", "POPER")).alias("steps"))
        .collect()
    )
    causal_violations = 0
    for r in chain_step_periods:
        steps = sorted(r.steps, key=lambda s: s.STEP_NUMBER)
        popers = [int(s.POPER) for s in steps]
        if popers != sorted(popers):
            causal_violations += 1
    if causal_violations:
        print(f"[smoke] FAIL: {causal_violations} chain(s) have non-causal step ordering")
        return 4
    print(f"[smoke] step-POPER causality: all {len(chain_step_periods)} chains have monotonic step order ✓")

    # TP method distribution
    print("[smoke] TP method distribution:")
    flows.groupBy("TP_METHOD").count().orderBy(F.col("count").desc()).show(truncate=False)

    # AWREF prefix distribution — confirms goods + non-goods flow types are exercised
    print("[smoke] flow type distribution (by AWREF prefix):")
    (
        flows.withColumn("PREFIX", F.col("AWREF").substr(1, 2))
        .groupBy("PREFIX").count().orderBy("PREFIX").show(truncate=False)
    )

    # Controversy / APA preview
    n_apa = flows.filter(F.col("APA_FLAG") == True).count()
    n_chal = flows.filter(F.col("CHALLENGED_FLAG") == True).count()
    print(f"[smoke] APA-covered flows: {n_apa}; challenged flows: {n_chal}")

    # Year-end true-up rows
    tu_lines = acdoca.filter(F.col("AWREF").startswith("TU"))
    n_tu = tu_lines.count()
    print(f"[smoke] year-end trueup rows: {n_tu}")
    if n_tu > 0:
        tu_lines.select("RBUKRS", "RACCT", "WSL", "BLART", "POPER", "AWREF", "SGTXT").show(20, truncate=False)

    # Markup-by-role-pair
    print("[smoke] markup by role pair:")
    (flows.groupBy("SELLER_ROLE", "BUYER_ROLE")
     .agg(F.avg("MARKUP_RATE").alias("avg_markup"),
          F.count("*").alias("n"))
     .orderBy("SELLER_ROLE", "BUYER_ROLE")
     .show(truncate=False))

    out_path = Path("/tmp/sc_flows.json")
    export_supply_chain_json(flows, str(out_path))
    print(f"[smoke] flows exported to {out_path} ({n_flows} records)")
    print(f"[smoke] launch viewer: .venv-plan/bin/python -m acdoca_generator.dash_app.app --data {out_path}")

    # Segment P&L with functional opex split — operational TP signal
    if result.segment_pl_df is not None:
        print("[smoke] segment P&L (functional opex split):")
        result.segment_pl_df.select(
            "RBUKRS", "ROLE_CODE", "POPER",
            "revenue", "cogs",
            "opex_production", "opex_rd", "opex_sm", "opex_ga", "opex_dist",
            "operating_profit", "operating_margin",
        ).orderBy("RBUKRS", "POPER").show(20, truncate=False)

        # ROLE_CODE lives on the master, not on ACDOCA lines, so distribution per
        # role requires a join against the segment_pl output instead of acdoca directly.
        print("[smoke] RFAREA distribution (acdoca line counts):")
        acdoca.groupBy("RFAREA").count().orderBy("RFAREA").show(20, truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
