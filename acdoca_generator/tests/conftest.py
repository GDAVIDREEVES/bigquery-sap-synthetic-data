import os

import pytest
from pyspark.sql import SparkSession

from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

# Avoid flaky host resolution on ephemeral runners (Spark binds to hostname).
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

_SPARK_FIXTURES = frozenset({"spark", "shared_pharma_df", "tech_ic_default_df", "pharma_ic_default_df"})


def pytest_collection_modifyitems(config, items):
    for item in items:
        if _SPARK_FIXTURES.intersection(item.fixturenames):
            item.add_marker(pytest.mark.spark)


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


@pytest.fixture(scope="session")
def tech_ic_default_df(spark):
    """Technology template with ic_pct=None (industry default IC share)."""
    cfg = GenerationConfig(
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
    return generate_acdoca_dataframe(spark, cfg).acdoca_df


@pytest.fixture(scope="session")
def pharma_ic_default_df(spark):
    """Pharmaceutical template with ic_pct=None (industry default IC share)."""
    cfg = GenerationConfig(
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
    return generate_acdoca_dataframe(spark, cfg).acdoca_df
