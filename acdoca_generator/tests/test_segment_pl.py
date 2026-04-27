"""Segment-level P&L aggregation tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from acdoca_generator.aggregations.segment_pl import (
    PIVOT_BUCKETS,
    PNL_CATEGORIES,
    build_segment_pl,
    pnl_category,
)
from acdoca_generator.config.chart_of_accounts import ACCOUNT_RANGES, SAMPLE_GL


# ---------------------------------------------------------------------------
# Fast (non-spark) tests for pnl_category
# ---------------------------------------------------------------------------


def test_pnl_category_for_each_account_range() -> None:
    """Every ACCOUNT_RANGES bucket maps midpoint values to the expected category."""
    for lo, hi, cat in ACCOUNT_RANGES:
        midpoint = (lo + hi) // 2
        assert pnl_category(str(midpoint)) == cat, (
            f"midpoint {midpoint} of {cat} range [{lo},{hi}] returned wrong category"
        )


def test_pnl_category_handles_leading_zeros() -> None:
    assert pnl_category("0800000") == "revenue"
    assert pnl_category("0925000") == "opex"
    assert pnl_category("0010000") == "cash_banks"


def test_pnl_category_unknown_for_out_of_range() -> None:
    assert pnl_category("0") == "unknown"
    assert pnl_category("999999999") == "unknown"


def test_pnl_category_handles_none_and_empty() -> None:
    assert pnl_category(None) == "unknown"
    assert pnl_category("") == "unknown"


def test_pnl_category_handles_non_numeric() -> None:
    assert pnl_category("ABC") == "unknown"
    assert pnl_category("12X34") == "unknown"


def test_pnl_categories_constant_excludes_balance_sheet() -> None:
    """The categories the aggregator pivots on are P&L-only."""
    bs_cats = {"cash_banks", "receivables", "inventory", "fixed_assets",
               "other_assets", "payables", "other_liabilities", "equity",
               "tax_extraordinary", "unknown"}
    assert set(PNL_CATEGORIES).isdisjoint(bs_cats)


def test_pnl_categories_constant_matches_account_ranges() -> None:
    """Every category in PNL_CATEGORIES is a real ACCOUNT_RANGES category."""
    range_cats = {cat for _lo, _hi, cat in ACCOUNT_RANGES}
    for cat in PNL_CATEGORIES:
        assert cat in range_cats, f"PNL_CATEGORIES has unknown category {cat!r}"


def test_pivot_buckets_partition_pnl_categories() -> None:
    """PIVOT_BUCKETS replaces 'opex' with 5 functional buckets; everything else identical."""
    expected_opex_split = {"opex_production", "opex_rd", "opex_sm", "opex_ga", "opex_dist"}
    assert set(PIVOT_BUCKETS) == (set(PNL_CATEGORIES) - {"opex"}) | expected_opex_split


def test_sample_gl_revenue_classifies_as_revenue() -> None:
    """SAMPLE_GL revenue accounts are in the revenue range."""
    for k in ("revenue_tp", "revenue_service", "revenue_ic"):
        assert pnl_category(SAMPLE_GL[k]) == "revenue", k
    assert pnl_category(SAMPLE_GL["cogs"]) == "cogs"
    assert pnl_category(SAMPLE_GL["cogs_ic"]) == "cogs"
    assert pnl_category(SAMPLE_GL["opex_rd"]) == "opex"
    assert pnl_category(SAMPLE_GL["opex_sga"]) == "opex"
    assert pnl_category(SAMPLE_GL["mgmt_fee_exp"]) == "ic_charges"
    assert pnl_category(SAMPLE_GL["royalty_exp"]) == "ic_charges"
    assert pnl_category(SAMPLE_GL["interest_inc"]) == "other_income"


# ---------------------------------------------------------------------------
# Spark-marked tests for build_segment_pl
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test_segment_pl")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def _make_companies(spark, rows):
    """Build a tiny company-master DF with just the columns build_segment_pl needs."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([
        StructField("RBUKRS", StringType(), True),
        StructField("ROLE_CODE", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def _make_acdoca(spark, rows):
    """Build a tiny ACDOCA-shape DF with only the columns build_segment_pl reads.

    The aggregator needs RBUKRS, SEGMENT, GJAHR, POPER, RACCT, RFAREA, KSL.
    """
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("RBUKRS", StringType(), True),
        StructField("SEGMENT", StringType(), True),
        StructField("GJAHR", IntegerType(), True),
        StructField("POPER", StringType(), True),
        StructField("RACCT", StringType(), True),
        StructField("RFAREA", StringType(), True),
        StructField("KSL", DecimalType(23, 2), True),
    ])
    return spark.createDataFrame(rows, schema)


@pytest.mark.spark
def test_build_segment_pl_revenue_positive(spark_session) -> None:
    """Revenue is sign-flipped to display positive; COGS is left positive."""
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-1000.00")),  # revenue (credit)
        ("1000", "SEG1", 2025, "001", "0900000", "0100", Decimal("600.00")),    # cogs (debit)
    ])
    companies = _make_companies(spark, [("1000", "LRD")])
    pl = build_segment_pl(acdoca, companies).collect()
    assert len(pl) == 1
    row = pl[0]
    assert row.revenue == Decimal("1000.00")
    assert row.cogs == Decimal("600.00")
    assert row.operating_profit == Decimal("400.00")


@pytest.mark.spark
def test_build_segment_pl_operating_profit_arithmetic(spark_session) -> None:
    """operating_profit = revenue + other_income - cogs - opex - ic_charges - depreciation."""
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-2000.00")),  # revenue
        ("1000", "SEG1", 2025, "001", "0860000", "0300", Decimal("-100.00")),   # other_income (interest)
        ("1000", "SEG1", 2025, "001", "0900000", "0100", Decimal("800.00")),    # cogs
        ("1000", "SEG1", 2025, "001", "0925000", "0400", Decimal("150.00")),    # opex (R&D)
        ("1000", "SEG1", 2025, "001", "0950000", "0300", Decimal("50.00")),     # ic_charges (mgmt fee)
        ("1000", "SEG1", 2025, "001", "0970000", "0300", Decimal("75.00")),     # depreciation
    ])
    companies = _make_companies(spark, [("1000", "IPPR")])
    row = build_segment_pl(acdoca, companies).collect()[0]
    expected = Decimal("2000.00") + Decimal("100.00") - Decimal("800.00") - Decimal("150.00") - Decimal("50.00") - Decimal("75.00")
    assert row.operating_profit == expected
    rev_total = row.revenue + row.other_income
    assert row.operating_margin == (row.operating_profit / rev_total).quantize(Decimal("0.000001"))


@pytest.mark.spark
def test_build_segment_pl_role_code_attached(spark_session) -> None:
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-100.00")),
        ("3000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-200.00")),
    ])
    companies = _make_companies(spark, [("1000", "IPPR"), ("3000", "LRD")])
    rows = {r.RBUKRS: r.ROLE_CODE for r in build_segment_pl(acdoca, companies).collect()}
    assert rows == {"1000": "IPPR", "3000": "LRD"}


@pytest.mark.spark
def test_build_segment_pl_grain_uniqueness(spark_session) -> None:
    """Group key (RBUKRS, ROLE_CODE, SEGMENT, GJAHR, POPER) is unique."""
    from pyspark.sql import functions as F

    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-100.00")),
        ("1000", "SEG1", 2025, "001", "0900000", "0100", Decimal("60.00")),
        ("1000", "SEG1", 2025, "002", "0800000", "0200", Decimal("-110.00")),
        ("3000", "SEG1", 2025, "001", "0800000", "0200", Decimal("-200.00")),
    ])
    companies = _make_companies(spark, [("1000", "IPPR"), ("3000", "LRD")])
    pl = build_segment_pl(acdoca, companies)
    counts = (
        pl.groupBy("RBUKRS", "ROLE_CODE", "SEGMENT", "GJAHR", "POPER")
        .agg(F.count("*").alias("n"))
        .collect()
    )
    assert all(r.n == 1 for r in counts), counts


@pytest.mark.spark
def test_build_segment_pl_excludes_balance_sheet(spark_session) -> None:
    """Cash / inventory / payables postings produce no P&L rows."""
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0010000", "", Decimal("500.00")),  # cash (balance sheet)
        ("1000", "SEG1", 2025, "001", "0220000", "", Decimal("300.00")),  # inventory
        ("1000", "SEG1", 2025, "001", "0500000", "", Decimal("-800.00")), # ap_trade
    ])
    companies = _make_companies(spark, [("1000", "LRD")])
    rows = build_segment_pl(acdoca, companies).collect()
    assert rows == []


@pytest.mark.spark
def test_build_segment_pl_zero_revenue_margin_is_null(spark_session) -> None:
    """Entity with only opex (no revenue) → operating_margin IS NULL."""
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0925000", "0400", Decimal("100.00")),  # opex (R&D)
    ])
    companies = _make_companies(spark, [("1000", "RDSC")])
    row = build_segment_pl(acdoca, companies).collect()[0]
    assert row.revenue == Decimal("0.00")
    assert row.other_income == Decimal("0.00")
    assert row.operating_margin is None
    assert row.operating_profit == Decimal("-100.00")
    assert row.opex_rd == Decimal("100.00")  # routed to opex_rd by RFAREA=0400


@pytest.mark.spark
def test_build_segment_pl_opex_split_by_rfarea(spark_session) -> None:
    """Five opex rows on the same GL with different RFAREA each land in their bucket."""
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0930000", "0100", Decimal("11.00")),  # production
        ("1000", "SEG1", 2025, "001", "0930000", "0200", Decimal("22.00")),  # sales -> opex_sm
        ("1000", "SEG1", 2025, "001", "0930000", "0500", Decimal("33.00")),  # marketing -> opex_sm
        ("1000", "SEG1", 2025, "001", "0930000", "0300", Decimal("44.00")),  # admin
        ("1000", "SEG1", 2025, "001", "0930000", "0400", Decimal("55.00")),  # R&D
        ("1000", "SEG1", 2025, "001", "0930000", "0600", Decimal("66.00")),  # distribution
    ])
    companies = _make_companies(spark, [("1000", "ENTR")])
    row = build_segment_pl(acdoca, companies).collect()[0]
    assert row.opex_production == Decimal("11.00")
    assert row.opex_sm == Decimal("55.00")  # 22 + 33 (sales + marketing)
    assert row.opex_ga == Decimal("44.00")
    assert row.opex_rd == Decimal("55.00")
    assert row.opex_dist == Decimal("66.00")
    # Old 'opex' column should not exist
    assert "opex" not in row.asDict()


@pytest.mark.spark
def test_build_segment_pl_blank_rfarea_falls_to_ga(spark_session) -> None:
    spark = spark_session
    acdoca = _make_acdoca(spark, [
        ("1000", "SEG1", 2025, "001", "0930000", "", Decimal("100.00")),
    ])
    companies = _make_companies(spark, [("1000", "IPPR")])
    row = build_segment_pl(acdoca, companies).collect()[0]
    assert row.opex_ga == Decimal("100.00")
