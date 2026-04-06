"""Fast, Spark-free paths for synthetic ACDOCA generation (optional Polars)."""

from __future__ import annotations

from acdoca_generator.core.companies_polars import build_companies_indexed_polars
from acdoca_generator.core.domestic_polars import domestic_balanced_polars

__all__: list[str] = [
    "build_companies_indexed_polars",
    "domestic_balanced_polars",
]
