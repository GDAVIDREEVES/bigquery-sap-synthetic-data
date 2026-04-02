#!/usr/bin/env python3
"""
Generate synthetic ACDOCA data and write to BigQuery via the Spark BigQuery connector.

Use on Dataproc, Cloud Spark, or local Spark with GCP credentials and a GCS staging bucket.

Example:
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
  export ACDOCA_GCS_TEMP_BUCKET=my-staging-bucket
  python scripts/run_generate_bq.py \\
    --full-table-name myproj.synthetic_acdoca.journal_entries \\
    --industry-key consumer_goods \\
    --country-isos US,DE,GB
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import asdict
from pathlib import Path

# Repo root when running `python scripts/run_generate_bq.py`
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Spark 3.5 + connector version (see https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
_SPARK_BQ_PACKAGE = os.environ.get(
    "ACDOCA_SPARK_BQ_PACKAGE",
    "com.google.cloud.spark:spark-3.5-bigquery:0.44.1",
)


def _spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("acdoca_generate_bigquery")
        .config("spark.jars.packages", _SPARK_BQ_PACKAGE)
        .getOrCreate()
    )


def _csv_list(v: str) -> list[str]:
    items = [x.strip() for x in (v or "").split(",")]
    return [x for x in items if x]


def main() -> int:
    p = argparse.ArgumentParser(description="ACDOCA synthetic generator → BigQuery")
    p.add_argument("--preset", default="custom", help="custom | quick_smoke | tp_workshop | globe_lite | ml_features")
    p.add_argument("--validation-profile", default="strict", choices=("strict", "fast"))
    p.add_argument("--industry-key", default="consumer_goods")
    p.add_argument("--country-isos", default="US,DE,GB", help="Comma-separated ISO codes")
    p.add_argument("--fiscal-year", type=int, default=2026)
    p.add_argument("--fiscal-variant", default="calendar", choices=("calendar", "april"))
    p.add_argument("--complexity", default="light", choices=("light", "medium", "high", "very_high"))
    p.add_argument("--txn-per-cc-per-period", type=int, default=1000)
    p.add_argument(
        "--ic-pct",
        default="0.25",
        help="Intercompany fraction 0..1; empty string uses industry template (custom preset only)",
    )
    p.add_argument("--include-reversals", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--include-closing", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--full-table-name",
        required=True,
        help="BigQuery table id: project.dataset.table",
    )
    p.add_argument(
        "--gcs-temp-bucket",
        default="",
        help="GCS bucket for connector staging (or set ACDOCA_GCS_TEMP_BUCKET)",
    )
    args = p.parse_args()

    from acdoca_generator.config.industries import canonical_industry_key
    from acdoca_generator.config.presets import get_preset
    from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
    from acdoca_generator.utils.spark_writer import GenerationParams, write_acdoca_table
    from acdoca_generator.validators.balance import blocking_failures, run_validations

    preset = args.preset.strip().lower()
    validation_profile = args.validation_profile
    industry_key = args.industry_key
    country_isos_csv = args.country_isos
    fiscal_year = args.fiscal_year
    fiscal_variant = args.fiscal_variant
    complexity = args.complexity
    txn_per_cc_per_period = args.txn_per_cc_per_period
    include_reversals = args.include_reversals
    include_closing = args.include_closing
    seed = args.seed

    if preset != "custom":
        pr = get_preset(preset)
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
        ic_pct = None if str(args.ic_pct).strip() == "" else float(args.ic_pct)

    country_isos = _csv_list(country_isos_csv)
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

    gcs_bucket = (args.gcs_temp_bucket or os.environ.get("ACDOCA_GCS_TEMP_BUCKET") or "").strip()
    if not gcs_bucket:
        print(
            "Error: set --gcs-temp-bucket or environment variable ACDOCA_GCS_TEMP_BUCKET.",
            file=sys.stderr,
        )
        return 2

    print("GenerationConfig:", asdict(cfg))
    print(f"preset={preset!r} validation_profile={validation_profile!r}")
    print(f"BigQuery table={args.full_table_name!r} gcs_temp_bucket={gcs_bucket!r}")

    spark = _spark()
    df = generate_acdoca_dataframe(spark, cfg)

    results = run_validations(df, profile=validation_profile)
    fails = blocking_failures(results)
    print("Validation results:")
    for r in results:
        print(f"- {r.name}: {r.display_severity} ({'PASS' if r.passed else 'FAIL'}) — {r.detail}")
    if fails:
        print("Blocking validation failures; write skipped.", file=sys.stderr)
        return 1

    write_acdoca_table(
        spark,
        df,
        full_table_name=args.full_table_name,
        gen=GenerationParams(
            industry=canonical_industry_key(industry_key),
            complexity=complexity,
            countries_iso_csv=",".join(country_isos),
            fiscal_year=int(fiscal_year),
            seed=int(seed),
            validation_profile=validation_profile,
        ),
        output_format="bigquery",
        gcs_temp_bucket=gcs_bucket,
    )
    print(f"Write complete (bigquery). Target: {args.full_table_name}")

    from pyspark.sql import functions as F

    total_rows = df.count()
    print(f"Total rows: {total_rows:,}")
    by_cc = df.groupBy("RBUKRS").count().orderBy("RBUKRS").collect()
    for row in by_cc:
        print(f"  {row['RBUKRS']}: {row['count']:,}")
    deb = df.filter(df.DRCRK == "S").select(F.sum("WSL")).collect()[0][0]
    cred = df.filter(df.DRCRK == "H").select(F.sum("WSL")).collect()[0][0]
    deb = float(deb or 0)
    cred = float(cred or 0)
    print(f"Sum WSL debits (S): {deb} / credits (H): {cred} (should net to ~0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
