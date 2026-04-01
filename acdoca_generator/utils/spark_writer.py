"""Delta / Parquet writer and table properties (SPEC §9)."""

from __future__ import annotations

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
    """Metadata persisted as Delta TBLPROPERTIES (SPEC §9.3)."""

    industry: str
    complexity: str
    countries_iso_csv: str
    fiscal_year: int
    seed: int
    version: str = ""
    validation_profile: str = ""


def _escape_prop(v: str) -> str:
    return v.replace("'", "''")


def write_acdoca_table(
    spark: SparkSession,
    df: DataFrame,
    *,
    full_table_name: str,
    gen: GenerationParams,
    output_format: str = "delta",
    parquet_path: Optional[str] = None,
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
    else:
        raise ValueError(f"Unknown output_format {output_format!r}")
