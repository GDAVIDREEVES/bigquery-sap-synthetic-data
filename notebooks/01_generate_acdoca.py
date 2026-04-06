#!/usr/bin/env python
# Databricks notebook source

# COMMAND ----------
# If running from Databricks Repos, install the package in editable mode.
# If you installed a wheel as a cluster library, you can remove this cell.
#
# %pip install -e /Workspace/Repos/<org>/<repo>/databricks-sap-synthetic-data
# dbutils.library.restartPython()

# COMMAND ----------
from __future__ import annotations

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


def _get_param_float(name: str, default: float) -> float:
    v = _get_param(name, str(default))
    return float(v)


def _get_param_bool(name: str, default: bool) -> bool:
    v = _get_param(name, "true" if default else "false").strip().lower()
    return v in {"1", "true", "t", "yes", "y"}


def _csv_list(v: str) -> list[str]:
    items = [x.strip() for x in (v or "").split(",")]
    return [x for x in items if x]


# COMMAND ----------
# Parameters (widgets in notebook UI; job parameters can override).
preset = _get_param("preset", "custom")  # custom | quick_smoke | tp_workshop | globe_lite | ml_features | supply_chain_demo
validation_profile = _get_param("validation_profile", "strict")  # strict | fast

industry_key = _get_param("industry_key", "consumer_goods")
country_isos_csv = _get_param("country_isos_csv", "US,DE,GB")
fiscal_year = _get_param_int("fiscal_year", 2026)
fiscal_variant = _get_param("fiscal_variant", "calendar")  # calendar | april
complexity = _get_param("complexity", "light")  # light | medium | high | very_high

txn_per_cc_per_period = _get_param_int("txn_per_cc_per_period", 1000)
ic_pct_raw = _get_param("ic_pct", "0.25")  # empty string → industry template default
include_reversals = _get_param_bool("include_reversals", True)
include_closing = _get_param_bool("include_closing", True)
seed = _get_param_int("seed", 42)
include_supply_chain = _get_param_bool("include_supply_chain", False)
sc_chains_per_period = _get_param_int("sc_chains_per_period", 50)

full_table_name = _get_param("full_table_name", "synthetic.acdoca.journal_entries")
output_format = _get_param("output_format", "delta")  # delta | parquet
parquet_path = _get_param("parquet_path", "/tmp/acdoca_synthetic")

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
    include_supply_chain = pr.include_supply_chain
    sc_chains_per_period = pr.sc_chains_per_period
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
    include_supply_chain=bool(include_supply_chain),
    sc_chains_per_period=int(sc_chains_per_period),
)

print("GenerationConfig:")
print(asdict(cfg))
print(f"preset={preset!r} validation_profile={validation_profile!r}")


# COMMAND ----------
_gen = generate_acdoca_dataframe(spark, cfg)  # type: ignore[name-defined]
df = _gen.acdoca_df


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
    output_format=output_format,
    parquet_path=parquet_path,
)

print(f"Write complete ({output_format}). Target: {full_table_name if output_format.lower() == 'delta' else parquet_path}")


# COMMAND ----------
# Quick smoke metrics
total_rows = df.count()
by_cc = df.groupBy("RBUKRS").count().orderBy("RBUKRS")

print(f"Total rows: {total_rows:,}")
display(by_cc)  # type: ignore[name-defined]

deb = df.filter(df.DRCRK == "S").select(F.sum("WSL")).collect()[0][0]
cred = df.filter(df.DRCRK == "H").select(F.sum("WSL")).collect()[0][0]
deb = float(deb or 0)
cred = float(cred or 0)
print(f"Sum WSL debits (S): {deb} / credits (H): {cred} (should net to ~0)")
