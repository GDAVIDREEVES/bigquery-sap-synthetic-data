"""Entity-roles master-file metadata for downstream operational TP consumers.

Pivots `companies_df.ROLE_CODE` into a flat per-RBUKRS table with boolean
discovery flags (IS_PRINCIPAL, IS_IP_OWNER, IS_FINANCING_ARM, IS_LRD, ...).
Lets a TP product answer "which entity is the principal here?" without
parsing flow rows or sniffing role-code strings.

Grain: one row per RBUKRS.
Output columns: RBUKRS, LAND1, ROLE_CODE, ROLE_DESCRIPTION, OM_LOW_PCT,
OM_HIGH_PCT, plus the IS_* flags below.

Flag taxonomy is grouped by TP function, not 1:1 with role codes — e.g.
IS_FULL_RISK_MFR covers both FRMF and LMFR; IS_DISTRIBUTOR covers LRD,
FFD, BSDIST, COMM, COMA. The ROLE_CODE column is preserved verbatim for
consumers that need finer granularity than the flags express.
"""

from __future__ import annotations

from typing import Dict, FrozenSet

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from acdoca_generator.config.operating_models import ROLE_BY_CODE


# Role-flag groupings. Keep aligned with the 20 roles in DEFAULT_ROLES.
_FLAG_MEMBERS: Dict[str, FrozenSet[str]] = {
    "IS_PRINCIPAL": frozenset({"IPPR", "ENTR"}),
    "IS_IP_OWNER": frozenset({"IPPR", "IPDEV"}),
    "IS_FINANCING_ARM": frozenset({"FINC"}),
    "IS_LRD": frozenset({"LRD"}),
    "IS_TOLL_MFR": frozenset({"TOLL"}),
    "IS_CONTRACT_MFR": frozenset({"CMFR"}),
    "IS_FULL_RISK_MFR": frozenset({"FRMF", "LMFR"}),
    "IS_RHQ": frozenset({"RHQ"}),
    "IS_SERVICE_PROVIDER": frozenset({"RDSC", "SSC", "RSP", "SSP"}),
    "IS_DISTRIBUTOR": frozenset({"LRD", "FFD", "BSDIST", "COMM", "COMA"}),
}

FLAG_COLUMNS: tuple[str, ...] = tuple(_FLAG_MEMBERS.keys())


def role_flags_for(role_code: str) -> Dict[str, bool]:
    """Return the IS_* flag dict for a given role code. Unknown codes get all False."""
    return {flag: role_code in members for flag, members in _FLAG_MEMBERS.items()}


def _role_lookup_df(spark: SparkSession) -> DataFrame:
    """Materialize the role catalog (one row per role code) for broadcast-joining."""
    fields = [
        StructField("ROLE_CODE", StringType(), nullable=False),
        StructField("ROLE_DESCRIPTION", StringType(), nullable=True),
        StructField("OM_LOW_PCT", DoubleType(), nullable=True),
        StructField("OM_HIGH_PCT", DoubleType(), nullable=True),
    ]
    fields.extend(StructField(f, BooleanType(), nullable=False) for f in FLAG_COLUMNS)
    schema = StructType(fields)
    rows = []
    for code, role in ROLE_BY_CODE.items():
        flags = role_flags_for(code)
        rows.append(
            (code, role.description, float(role.om_low_pct), float(role.om_high_pct))
            + tuple(flags[f] for f in FLAG_COLUMNS)
        )
    return spark.createDataFrame(rows, schema)


def build_entity_roles(companies_df: DataFrame) -> DataFrame:
    """Pivot companies_df into the per-entity master-file metadata table.

    Parameters
    ----------
    companies_df : DataFrame
        Must have at least RBUKRS, LAND1, ROLE_CODE columns
        (per `master_data.build_companies`).

    Returns
    -------
    DataFrame with one row per RBUKRS and the columns documented at module level.
    """
    spark = companies_df.sparkSession
    lookup = _role_lookup_df(spark)
    out = (
        companies_df.select("RBUKRS", "LAND1", "ROLE_CODE")
        .join(F.broadcast(lookup), "ROLE_CODE", "left")
    )
    select_cols = [
        F.col("RBUKRS"),
        F.col("LAND1"),
        F.col("ROLE_CODE"),
        F.col("ROLE_DESCRIPTION"),
        F.col("OM_LOW_PCT"),
        F.col("OM_HIGH_PCT"),
    ] + [F.coalesce(F.col(f), F.lit(False)).alias(f) for f in FLAG_COLUMNS]
    return out.select(*select_cols).orderBy("RBUKRS")
