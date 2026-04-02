#!/usr/bin/env python
# Databricks notebook source
# Similar to 01_generate_acdoca.py but targets BigQuery (project.dataset.table) and GCS staging.
#
# Dataproc / Vertex Workbench: ensure the Spark BigQuery connector is on the classpath, e.g.:
#   SparkSession.builder.config(
#       "spark.jars.packages",
#       "com.google.cloud.spark:spark-3.5-bigquery:0.44.1",
#   )
# Or use a Dataproc image that includes the connector.
#
# Parameters: mirror the Databricks notebook; use dbutils.widgets when available, else defaults.

# COMMAND ----------
from __future__ import annotations

import os
from dataclasses import asdict

from pyspark.sql import functions as F

from acdoca_generator.config.industries import canonical_industry_key
from acdoca_generator.config.presets import get_preset
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.utils.spark_writer import GenerationParams, write_acdoca_table
from acdoca_generator.validators.balance import blocking_failures, run_validations


def _get_param(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


def _get_param_int(name: str, default: int) -> int:
    v = _get_param(name, str(default))
    return int(v)


def _get_param_bool(name: str, default: bool) -> bool:
    v = _get_param(name, "true" if default else "false").strip().lower()
    return v in {"1", "true", "t", "yes", "y"}


def _csv_list(v: str) -> list[str]:
    items = [x.strip() for x in (v or "").split(",")]
    return [x for x in items if x]


# COMMAND ----------
preset = _get_param("preset", "custom")
validation_profile = _get_param("validation_profile", "strict")

industry_key = _get_param("industry_key", "consumer_goods")
country_isos_csv = _get_param("country_isos_csv", "US,DE,GB")
fiscal_year = _get_param_int("fiscal_year", 2026)
fiscal_variant = _get_param("fiscal_variant", "calendar")
complexity = _get_param("complexity", "light")

txn_per_cc_per_period = _get_param_int("txn_per_cc_per_period", 1000)
ic_pct_raw = _get_param("ic_pct", "0.25")
include_reversals = _get_param_bool("include_reversals", True)
include_closing = _get_param_bool("include_closing", True)
seed = _get_param_int("seed", 42)

full_table_name = _get_param("full_table_name", "my-gcp-project.synthetic_acdoca.journal_entries")
gcs_temp_bucket = _get_param("gcs_temp_bucket", os.environ.get("ACDOCA_GCS_TEMP_BUCKET", ""))

if preset.strip().lower() != "custom":
    pr = get_preset(preset.strip().lower())
    industry_key = pr.industry_key
    country_isos_csv = pr.country_isos_csv
    fiscal_year = pr.fiscal_year
    fiscal_variant = pr.fiscal_variant
    complexity = pr.complexity
    txn_per_cc_per_period = pr.txn_per_cc_per_period
    ic_pct = pr.ic_pct
    include_reversals = pr.include_reversals
    include_closing = pr.include_closing
    validation_profile = pr.validation_profile
else:
    ic_pct = None if str(ic_pct_raw).strip() == "" else float(ic_pct_raw)

country_isos = _csv_list(country_isos_csv)

# COMMAND ----------
cfg = GenerationConfig(
    industry_key=industry_key,
    country_isos=country_isos,
    fiscal_year=int(fiscal_year),
    fiscal_variant=str(fiscal_variant),
    complexity=str(complexity),
    txn_per_cc_per_period=int(txn_per_cc_per_period),
    ic_pct=ic_pct,
    include_reversals=bool(include_reversals),
    include_closing=bool(include_closing),
    seed=int(seed),
)

print("GenerationConfig:")
print(asdict(cfg))
print(f"preset={preset!r} validation_profile={validation_profile!r}")
print(f"full_table_name={full_table_name!r} gcs_temp_bucket={gcs_temp_bucket!r}")

# COMMAND ----------
if not str(gcs_temp_bucket).strip():
    raise RuntimeError("Set gcs_temp_bucket widget or ACDOCA_GCS_TEMP_BUCKET for BigQuery staging.")

df = generate_acdoca_dataframe(spark, cfg)  # type: ignore[name-defined]

# COMMAND ----------
results = run_validations(df, profile=validation_profile)
fails = blocking_failures(results)

print("Validation results:")
for r in results:
    print(f"- {r.name}: {r.display_severity} ({'PASS' if r.passed else 'FAIL'}) — {r.detail}")

if fails:
    raise RuntimeError("Blocking validation failures; write skipped.")

# COMMAND ----------
write_acdoca_table(
    spark,  # type: ignore[name-defined]
    df,
    full_table_name=full_table_name,
    gen=GenerationParams(
        industry=canonical_industry_key(industry_key),
        complexity=complexity,
        countries_iso_csv=",".join(country_isos),
        fiscal_year=int(fiscal_year),
        seed=int(seed),
        validation_profile=validation_profile,
    ),
    output_format="bigquery",
    gcs_temp_bucket=str(gcs_temp_bucket).strip(),
)

print(f"Write complete (bigquery). Target: {full_table_name}")

# COMMAND ----------
total_rows = df.count()
by_cc = df.groupBy("RBUKRS").count().orderBy("RBUKRS")

print(f"Total rows: {total_rows:,}")
try:
    display(by_cc)  # type: ignore[name-defined]
except Exception:
    by_cc.show(200)

deb = df.filter(df.DRCRK == "S").select(F.sum("WSL")).collect()[0][0]
cred = df.filter(df.DRCRK == "H").select(F.sum("WSL")).collect()[0][0]
deb = float(deb or 0)
cred = float(cred or 0)
print(f"Sum WSL debits (S): {deb} / credits (H): {cred} (should net to ~0)")
