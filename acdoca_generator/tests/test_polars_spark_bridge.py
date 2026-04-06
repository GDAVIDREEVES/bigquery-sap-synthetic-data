"""Polars → Spark bridge and domestic volume parity with Spark (integration)."""

from __future__ import annotations

import polars as pl
import pytest
from pyspark.sql import SparkSession

from acdoca_generator.config.industries import get_industry
from acdoca_generator.core.domestic_polars import domestic_balanced_polars
from acdoca_generator.generators.master_data import build_companies
from acdoca_generator.generators.pipeline import _companies_indexed_with_fx
from acdoca_generator.generators.transactions import domestic_balanced_documents
from acdoca_generator.spark_bridge import polars_to_spark


@pytest.mark.spark
def test_polars_to_spark_roundtrip(spark: SparkSession):
    pdf = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    sdf = polars_to_spark(spark, pdf)
    assert sdf.count() == 2


@pytest.mark.spark
def test_domestic_polars_matches_spark_row_count(spark: SparkSession):
    industry = get_industry("pharmaceutical")
    country_isos = ["US", "DE"]
    seed = 11
    gjahr = 2026
    n_docs = 40
    n_comp = len(country_isos)

    companies = build_companies(spark, country_isos, "pharmaceutical", seed)
    cidx = _companies_indexed_with_fx(spark, companies, "USD", seed)

    sp_df = domestic_balanced_documents(
        spark,
        n_docs,
        cidx,
        gjahr,
        seed,
        "USD",
        False,
        industry,
        n_comp=n_comp,
    )
    pl_df = domestic_balanced_polars(
        n_docs,
        country_isos=country_isos,
        industry_key="pharmaceutical",
        gjahr=gjahr,
        seed=seed,
        group_currency="USD",
        include_reversals=False,
        industry=industry,
    )
    assert sp_df is not None and pl_df is not None
    assert sp_df.count() == len(pl_df) == n_docs * 2
