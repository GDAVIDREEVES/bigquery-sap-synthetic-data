from pyspark.sql import SparkSession

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from pyspark.sql import functions as F


def test_ic_awref_pairs_two_company_codes(spark: SparkSession):
    cfg = GenerationConfig(
        industry_key="technology",
        country_isos=["US", "DE", "GB"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=80,
        ic_pct=0.4,
        include_reversals=False,
        include_closing=False,
        seed=11,
    )
    df = generate_acdoca_dataframe(spark, cfg)
    ic = df.filter(F.col("RASSC") != "")
    if ic.limit(1).count() == 0:
        return
    paired = (
        ic.groupBy("AWREF")
        .agg(F.countDistinct("RBUKRS").alias("cc"))
        .filter(F.col("cc") >= 2)
        .count()
    )
    aw = ic.select("AWREF").distinct().count()
    assert paired == aw
