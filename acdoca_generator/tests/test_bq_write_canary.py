"""End-to-end BigQuery write canary (Phase B.2 reliability gate).

Generates the ``globe_lite`` preset (~30K rows, multi-country, fast) and writes
it to a real BigQuery table via the Spark BQ connector. Verifies the round-trip
by issuing a ``SELECT COUNT(*)`` against the written table, then drops the table.

Gated by *both* ``ACDOCA_RUN_SPARK_TESTS=1`` and ``ACDOCA_RUN_BQ_TESTS=1`` so an
accidental local pytest run cannot bill the user. Reads target project/dataset
and the GCS staging bucket from env vars:

    ACDOCA_BQ_TABLE          project.dataset.table  (table name suffix gets a
                                                     unique timestamp appended)
    ACDOCA_GCS_TEMP_BUCKET   GCS bucket the connector uses to stage temp files
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest
from pyspark.sql import SparkSession

from acdoca_generator.config.industries import canonical_industry_key
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.utils.spark_writer import GenerationParams, write_acdoca_table


pytestmark = pytest.mark.skipif(
    os.environ.get("ACDOCA_RUN_BQ_TESTS") != "1",
    reason="set ACDOCA_RUN_BQ_TESTS=1 to enable real-BigQuery canary",
)


def _bq_query(sql: str) -> list[str]:
    """Run `bq query` with the standard CLI; return CSV lines minus the header row."""
    out = subprocess.check_output(
        [
            "bq",
            "query",
            "--use_legacy_sql=false",
            "--format=csv",
            "--quiet",
            sql,
        ],
        text=True,
    )
    return out.strip().splitlines()[1:]


def _bq_count(table_id: str) -> int:
    rows = _bq_query(f"SELECT COUNT(*) AS n FROM `{table_id}`")
    return int(rows[-1])


def _bq_column_description(table_id: str, column: str) -> str:
    """Read `description` for one column from INFORMATION_SCHEMA.COLUMNS."""
    project, dataset, table = table_id.split(".")
    sql = (
        f"SELECT description FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' AND column_name = '{column}'"
    )
    rows = _bq_query(sql)
    return rows[-1].strip('"') if rows else ""


def _bq_drop(table_id: str) -> None:
    subprocess.run(["bq", "rm", "-f", "-t", table_id], check=False)


def test_bq_write_canary_globe_lite() -> None:
    base_table = (os.environ.get("ACDOCA_BQ_TABLE") or "").strip()
    bucket = (os.environ.get("ACDOCA_GCS_TEMP_BUCKET") or "").strip()
    if not base_table or not bucket:
        pytest.skip("set ACDOCA_BQ_TABLE and ACDOCA_GCS_TEMP_BUCKET to run BQ canary")

    if base_table.count(".") != 2:
        pytest.fail("ACDOCA_BQ_TABLE must be project.dataset.table")

    table_id = f"{base_table}_canary_{int(time.time())}"
    project, dataset, _table = table_id.split(".")

    spark_bq_pkg = os.environ.get(
        "ACDOCA_SPARK_BQ_PACKAGE", "com.google.cloud.spark:spark-3.5-bigquery:0.44.1"
    )
    gcs_jar = os.environ.get(
        "ACDOCA_SPARK_GCS_JAR",
        os.path.expanduser("~/.spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar"),
    )

    builder = (
        SparkSession.builder.appName("acdoca_bq_canary")
        .config("spark.jars.packages", spark_bq_pkg)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    )
    if os.path.exists(gcs_jar):
        builder = builder.config("spark.jars", gcs_jar)
    spark = builder.getOrCreate()

    cfg = GenerationConfig(
        industry_key="media",
        country_isos=["US", "GB", "FR", "IN"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=600,
        ic_pct=None,
        include_reversals=True,
        include_closing=True,
        seed=42,
    )

    try:
        result = generate_acdoca_dataframe(spark, cfg)
        df = result.acdoca_df
        expected = df.count()
        assert expected > 0

        write_acdoca_table(
            spark,
            df,
            full_table_name=table_id,
            gen=GenerationParams(
                industry=canonical_industry_key(cfg.industry_key),
                complexity=cfg.complexity,
                countries_iso_csv=",".join(cfg.country_isos),
                fiscal_year=cfg.fiscal_year,
                seed=cfg.seed,
                validation_profile="fast",
            ),
            output_format="bigquery",
            gcs_temp_bucket=bucket,
        )

        observed = _bq_count(table_id)
        assert observed == expected, f"BQ row count {observed} != expected {expected}"

        # Schema metadata round-trip — confirms StructField description rode along
        # to BigQuery's INFORMATION_SCHEMA via the connector.
        rbukrs_desc = _bq_column_description(table_id, "RBUKRS")
        assert rbukrs_desc == "Company Code", (
            f"BQ description for RBUKRS = {rbukrs_desc!r}, expected 'Company Code'"
        )
    finally:
        _bq_drop(table_id)
        spark.stop()
