"""Fast Polars domestic generator (no Spark)."""

from __future__ import annotations

import polars as pl
import pytest

from acdoca_generator.config.industries import get_industry
from acdoca_generator.core.domestic_polars import domestic_balanced_polars


@pytest.fixture(scope="module")
def pharma_industry():
    return get_industry("pharmaceutical")


def test_domestic_polars_row_count_and_balance(pharma_industry):
    n_docs = 25
    df = domestic_balanced_polars(
        n_docs,
        country_isos=["US", "DE"],
        industry_key="pharmaceutical",
        gjahr=2026,
        seed=42,
        group_currency="USD",
        include_reversals=False,
        industry=pharma_industry,
    )
    assert df is not None
    assert len(df) == n_docs * 2
    by_doc = df.group_by("BELNR").agg(pl.col("WSL").cast(pl.Float64).sum().alias("net"))
    assert (by_doc["net"].abs() < 0.01).all()


def test_domestic_polars_reversals_flag_present(pharma_industry):
    df = domestic_balanced_polars(
        50,
        country_isos=["US"],
        industry_key="pharmaceutical",
        gjahr=2026,
        seed=1,
        group_currency="USD",
        include_reversals=True,
        industry=pharma_industry,
    )
    assert df is not None
    assert "XREVERSING" in df.columns
