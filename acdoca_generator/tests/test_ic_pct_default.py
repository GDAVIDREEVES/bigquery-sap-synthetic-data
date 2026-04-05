from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe


def test_ic_pct_none_uses_industry_default(spark: SparkSession):
    tech = GenerationConfig(
        industry_key="technology",
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=20,
        ic_pct=None,
        include_reversals=False,
        include_closing=False,
        seed=99,
    )
    pharma = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=20,
        ic_pct=None,
        include_reversals=False,
        include_closing=False,
        seed=99,
    )
    d_t = generate_acdoca_dataframe(spark, tech).acdoca_df
    d_p = generate_acdoca_dataframe(spark, pharma).acdoca_df
    ic_t = d_t.filter(F.col("RASSC") != "").count()
    ic_p = d_p.filter(F.col("RASSC") != "").count()
    assert ic_t > ic_p, f"technology default IC share > pharma: {ic_t} vs {ic_p}"
