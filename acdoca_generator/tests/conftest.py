import os

import pytest
from pyspark.sql import SparkSession

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

# Avoid flaky host resolution on ephemeral runners (Spark binds to hostname).
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    try:
        s = (
            SparkSession.builder.master("local[2]")
            .appName("acdoca_tests")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        s.range(1).count()
    except Exception as exc:  # noqa: BLE001 — surface skip reason in CI without Java
        pytest.skip(f"Spark session unavailable ({exc})")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def shared_pharma_df(spark):
    """Single generation reused by balance, IC-pairing, and validation-profile tests."""
    cfg = GenerationConfig(
        industry_key="pharmaceutical",
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=20,
        ic_pct=0.25,
        include_reversals=True,
        include_closing=False,
        seed=7,
    )
    return generate_acdoca_dataframe(spark, cfg).acdoca_df
