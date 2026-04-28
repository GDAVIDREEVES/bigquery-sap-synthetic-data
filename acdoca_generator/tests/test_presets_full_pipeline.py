"""End-to-end pipeline test for every demo preset (Phase B.1 reliability gate).

Each preset must:
  1. Generate without exception
  2. Produce non-zero rows
  3. Net to zero on WSL (debits + credits balance per `BELNR` document)

Phase A reduced catalyst-plan complexity so larger presets complete on a
modest runtime; this test guards against regression. Gated by
``ACDOCA_RUN_SPARK_TESTS=1`` so non-Spark CI runs stay fast.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.config.presets import DEMO_PRESETS, get_preset
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe


def _cfg_from_preset(preset_key: str) -> GenerationConfig:
    pr = get_preset(preset_key)
    return GenerationConfig(
        industry_key=pr.industry_key,
        country_isos=[c.strip() for c in pr.country_isos_csv.split(",") if c.strip()],
        fiscal_year=pr.fiscal_year,
        fiscal_variant=pr.fiscal_variant,
        complexity=pr.complexity,
        txn_per_cc_per_period=pr.txn_per_cc_per_period,
        ic_pct=pr.ic_pct,
        include_reversals=pr.include_reversals,
        include_closing=pr.include_closing,
        seed=42,
        include_supply_chain=pr.include_supply_chain,
        sc_chains_per_period=pr.sc_chains_per_period,
        include_segment_pl=pr.include_segment_pl,
        challenged_share=pr.challenged_share,
    )


@pytest.mark.parametrize("preset_key", sorted(DEMO_PRESETS.keys()))
def test_preset_full_pipeline(spark: SparkSession, preset_key: str) -> None:
    cfg = _cfg_from_preset(preset_key)
    result = generate_acdoca_dataframe(spark, cfg)
    df = result.acdoca_df

    n = df.count()
    assert n > 0, f"{preset_key} produced 0 rows"

    bad = (
        df.groupBy("BELNR")
        .agg(F.sum("WSL").alias("net"))
        .filter(F.abs(F.col("net")) > F.lit(Decimal("0.01")))
    )
    bad_n = bad.count()
    assert bad_n == 0, f"{preset_key}: {bad_n} unbalanced documents"
