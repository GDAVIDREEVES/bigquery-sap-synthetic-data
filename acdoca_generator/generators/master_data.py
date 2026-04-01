"""Synthetic master data: company codes, PCs, key accounts (SPEC §6)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.countries import get_country
from acdoca_generator.config.operating_models import pick_role_for_country


@dataclass(frozen=True)
class CompanyRow:
    rbukrs: str
    land1: str
    rhcur: str
    rwcur: str
    kokrs: str
    prctr: str
    rcntr: str
    segment: str
    role_code: str


def build_companies(
    spark: SparkSession, country_isos: List[str], industry_key: str, _seed: int
) -> DataFrame:
    """One company code per selected country; +1 BUKRS if duplicate base (SPEC §6.2)."""
    rows: list[CompanyRow] = []
    per_iso_count: dict[str, int] = {}
    for iso in sorted(country_isos):
        c = get_country(iso)
        role = pick_role_for_country(iso, industry_key)
        n = per_iso_count.get(iso, 0)
        per_iso_count[iso] = n + 1
        base = c.sample_bukrs
        code_int = base + n
        rbukrs = str(code_int).zfill(4)
        prctr = f"{rbukrs}{role.code}01"
        rcntr = f"{rbukrs}GADM01"
        rows.append(
            CompanyRow(
                rbukrs=rbukrs,
                land1=iso,
                rhcur=c.currency,
                rwcur=c.currency,
                kokrs=rbukrs,
                prctr=prctr,
                rcntr=rcntr,
                segment="SEG1",
                role_code=role.code,
            )
        )
    schema = StructType(
        [
            StructField("RBUKRS", StringType()),
            StructField("LAND1", StringType()),
            StructField("RHCUR", StringType()),
            StructField("RWCUR", StringType()),
            StructField("KOKRS", StringType()),
            StructField("PRCTR", StringType()),
            StructField("RCNTR", StringType()),
            StructField("SEGMENT", StringType()),
            StructField("ROLE_CODE", StringType()),
        ]
    )
    return spark.createDataFrame([tuple(vars(r).values()) for r in rows], schema)


