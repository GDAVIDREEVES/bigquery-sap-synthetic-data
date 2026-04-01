from pyspark.sql import SparkSession

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.validators.balance import blocking_failures, run_validations


def test_fast_profile_skips_pk_full_scan(spark: SparkSession):
    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=80,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=1,
    )
    df = generate_acdoca_dataframe(spark, cfg)
    results = run_validations(df, profile="fast")
    pk = next(r for r in results if r.name == "PK_UNIQUE")
    assert pk.passed
    assert pk.metrics.get("skipped") is True
    assert blocking_failures(results) == []
