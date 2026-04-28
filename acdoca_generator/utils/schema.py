"""Full ACDOCA Spark schema (SPEC §9.1)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config._field_descriptions_generated import FIELD_DESCRIPTIONS
from ..config._field_specs_generated import FIELD_SPECS

_STYPE_TO_SPARK = {
    "string": StringType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "decimal2": DecimalType(23, 2),
    "decimal3": DecimalType(23, 3),
    "int": IntegerType(),
}


def spark_type_for_spec(stype: str):
    return _STYPE_TO_SPARK[stype]


def acdoca_schema() -> StructType:
    """Canonical 538-column ACDOCA schema, with SAP descriptions on each field.

    The Spark BigQuery connector reads ``StructField.metadata["description"]``
    and writes it to the BigQuery column description on save, so descriptions
    appear automatically in the generated table without a post-write step.
    Lookup uses the SAP technical name (``sap``); fields absent from the XLSX
    source fall back to no metadata.
    """
    fields = []
    for sap, sql, _tier, stype in FIELD_SPECS:
        desc = FIELD_DESCRIPTIONS.get(sap, "")
        meta = {"description": desc} if desc else {}
        fields.append(StructField(sql, spark_type_for_spec(stype), nullable=True, metadata=meta))
    return StructType(fields)


def ordered_sql_column_names() -> list[str]:
    return [sql for _sap, sql, _t, _s in FIELD_SPECS]


def align_to_acdoca(df: DataFrame) -> DataFrame:
    """Reshape `df` to exactly match `acdoca_schema()`.

    Adds missing columns as typed nulls in a single ``select`` (one catalyst
    node) instead of per-column ``withColumn`` chains. Drops any extra columns
    not in the canonical schema. Reorders columns to match the schema.

    Call this once at the end of every generator that contributes to the
    pipeline accumulator so the orchestrator can ``unionByName`` without
    per-union re-alignment. Each call here adds ~1 catalyst node to the
    generator's lazy plan; without this helper, the orchestrator would add
    538 ``withColumn`` casts per union (~2,000 nodes for a 4-source pipeline).
    """
    schema = acdoca_schema()
    existing = set(df.columns)
    select_exprs = []
    for field in schema.fields:
        if field.name in existing:
            select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*select_exprs)
