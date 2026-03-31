from pyspark.sql import SparkSession

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.validators.balance import run_validations


def test_doc_balance_and_pk(spark: SparkSession):
    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="medium",
        txn_per_cc_per_period=50,
        ic_pct=0.25,
        include_reversals=True,
        include_closing=False,
        seed=7,
    )
    df = generate_acdoca_dataframe(spark, cfg)
    results = run_validations(df)
    by_name = {r.name: r for r in results}
    assert by_name["DOC_BALANCE"].passed, by_name["DOC_BALANCE"].detail
    assert by_name["PK_UNIQUE"].passed, by_name["PK_UNIQUE"].detail
    assert by_name["DRCRK_SIGN"].passed, by_name["DRCRK_SIGN"].detail
