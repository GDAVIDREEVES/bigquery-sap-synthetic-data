"""Delta / Parquet / BigQuery writer and table metadata (SPEC §9)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession


def _package_version() -> str:
    try:
        from importlib.metadata import version

        return version("acdoca-generator")
    except Exception:
        return "1.0.0"


@dataclass
class GenerationParams:
    """Metadata persisted as Delta TBLPROPERTIES or BigQuery table labels (SPEC §9.3)."""

    industry: str
    complexity: str
    countries_iso_csv: str
    fiscal_year: int
    seed: int
    version: str = ""
    validation_profile: str = ""


def _escape_prop(v: str) -> str:
    return v.replace("'", "''")


def _bigquery_table_id_parts(full_table_name: str) -> tuple[str, str, str]:
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            "BigQuery table id must be `project.dataset.table` (three dot-separated segments); "
            f"got {full_table_name!r}"
        )
    return parts[0], parts[1], parts[2]


def _bigquery_label_value(v: str) -> str:
    """BigQuery label values: lowercase, max 63 chars; allow a-z, 0-9, _, -."""
    s = str(v).lower().replace(",", "_").replace(" ", "_")
    s = re.sub(r"[^a-z0-9_-]+", "_", s).strip("_")
    if not s:
        s = "na"
    return s[:63]


def _bigquery_table_label_write_options(props: Dict[str, str]) -> Dict[str, str]:
    """Spark BigQuery connector options: bigQueryTableLabel.<key> = <value>."""
    opts: Dict[str, str] = {}
    for k, v in props.items():
        lk = k.replace(".", "_").lower()
        if not lk or not lk[0].isalpha():
            lk = f"g_{lk}" if lk else "g_meta"
        lk = lk[:63]
        opts[f"bigQueryTableLabel.{lk}"] = _bigquery_label_value(v)
    return opts


def write_acdoca_table(
    spark: SparkSession,
    df: DataFrame,
    *,
    full_table_name: str,
    gen: GenerationParams,
    output_format: str = "delta",
    parquet_path: Optional[str] = None,
    gcs_temp_bucket: Optional[str] = None,
) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ver = (gen.version or "").strip() or _package_version()
    props: Dict[str, str] = {
        "generator.version": ver,
        "generator.industry": gen.industry,
        "generator.complexity": gen.complexity,
        "generator.countries": gen.countries_iso_csv,
        "generator.fiscal_year": str(gen.fiscal_year),
        "generator.seed": str(gen.seed),
        "generator.timestamp": ts,
    }
    vp = (gen.validation_profile or "").strip()
    if vp:
        props["generator.validation_profile"] = vp
    fmt = (output_format or "delta").lower()
    if fmt == "delta":
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("RBUKRS", "GJAHR", "POPER")
            .saveAsTable(full_table_name)
        )
        parts = ", ".join(f"'{k}'='{_escape_prop(v)}'" for k, v in props.items())
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({parts})")
    elif fmt == "parquet":
        path = parquet_path or "/tmp/acdoca_synthetic_parquet"
        df.write.mode("overwrite").partitionBy("RBUKRS", "GJAHR", "POPER").parquet(path)
    elif fmt == "bigquery":
        _bigquery_table_id_parts(full_table_name)
        bucket = (gcs_temp_bucket or os.environ.get("ACDOCA_GCS_TEMP_BUCKET") or "").strip()
        if not bucket:
            raise ValueError(
                "BigQuery write requires gcs_temp_bucket=... or environment variable "
                "ACDOCA_GCS_TEMP_BUCKET (GCS bucket for connector staging)."
            )
        # INDIRECT write: time partition on BUDAT + cluster on company/year/period (BQ allows
        # clustering columns distinct from the partition column).
        label_opts = _bigquery_table_label_write_options(props)
        writer = (
            df.write.format("bigquery")
            .mode("overwrite")
            .option("temporaryGcsBucket", bucket)
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("partitionField", "BUDAT")
            .option("partitionType", "MONTH")
            .option("clusteredFields", "RBUKRS,GJAHR,POPER")
        )
        for opt_k, opt_v in label_opts.items():
            writer = writer.option(opt_k, opt_v)
        writer.save(full_table_name)
    else:
        raise ValueError(f"Unknown output_format {output_format!r}")
