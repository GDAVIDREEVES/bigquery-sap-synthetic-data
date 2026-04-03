import os

import pytest
from pyspark.sql import SparkSession

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
