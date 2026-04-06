from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe


def test_industry_changes_domestic_gl_mix(spark: SparkSession):
    """Technology template weights R&D higher than consumer_goods; expect more opex_rd postings."""
    base_kw = dict(
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=30,
        ic_pct=0.0,
        include_reversals=False,
        include_closing=False,
        seed=123,
    )
    df_tech = generate_acdoca_dataframe(
        spark,
        GenerationConfig(industry_key="technology", **base_kw),
    ).acdoca_df
    df_cpg = generate_acdoca_dataframe(
        spark,
        GenerationConfig(industry_key="consumer_goods", **base_kw),
    ).acdoca_df
    rd = SAMPLE_GL["opex_rd"]
    n_tech = df_tech.filter(F.col("RACCT") == F.lit(rd)).count()
    n_cpg = df_cpg.filter(F.col("RACCT") == F.lit(rd)).count()
    assert n_tech > n_cpg, f"expected tech rd lines > cpg, got {n_tech} vs {n_cpg}"
