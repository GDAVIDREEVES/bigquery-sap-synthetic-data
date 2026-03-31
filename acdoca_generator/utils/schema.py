"""Full ACDOCA Spark schema (SPEC §9.1)."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
    fields = [
        StructField(sql, spark_type_for_spec(stype), nullable=True)
        for _sap, sql, _tier, stype in FIELD_SPECS
    ]
    return StructType(fields)


def ordered_sql_column_names() -> list[str]:
    return [sql for _sap, sql, _t, _s in FIELD_SPECS]
