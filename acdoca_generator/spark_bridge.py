"""Convert Polars tables to Spark in one step for write / legacy paths."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from pyspark.sql import DataFrame, SparkSession


def polars_to_spark(spark: SparkSession, df: pl.DataFrame) -> DataFrame:
    """Materialize a Polars frame as a Spark DataFrame (single driver round-trip via pandas)."""
    return spark.createDataFrame(df.to_pandas())
