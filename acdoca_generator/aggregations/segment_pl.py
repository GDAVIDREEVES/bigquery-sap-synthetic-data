"""Segment-level P&L aggregation for operational TP analytics.

Rolls ACDOCA journal lines up to (RBUKRS, ROLE_CODE, SEGMENT, GJAHR, POPER) grain,
classifies each line by (GL-account category, RFAREA), and produces:
revenue / other_income / cogs / opex_production / opex_rd / opex_sm / opex_ga /
opex_dist / ic_charges / depreciation, plus operating profit and operating margin.

The opex GL bucket (ACCOUNT_RANGES "opex") is split by SAP functional area
(RFAREA) into five functional buckets driven by the line's function:
- 0100 Production -> opex_production
- 0400 R&D        -> opex_rd
- 0200 Sales / 0500 Marketing -> opex_sm
- 0600 Distribution -> opex_dist
- 0300 Administration / blank / unknown -> opex_ga (default)

KSL is already in group currency (FX-translated upstream), so it sums directly.
SAP DRCRK convention: revenue and other-income lines are credit (KSL negative);
COGS, opex, IC-charges, depreciation are debit (KSL positive). The aggregator
sign-flips revenue-side categories so the output reads as positive income.

Note: SEGMENT is currently constant ("SEG1") on every company in master_data.py,
so the segment dimension collapses today. The aggregator is correct; the data
shape just doesn't yet exercise multi-segment. Multi-segment population is a
separate realism gap.

Note: RFAREA is populated at complexity tier "M" (medium) and above. Light-tier
runs null RFAREA, in which case all opex falls into opex_ga via the default branch.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from acdoca_generator.config.chart_of_accounts import ACCOUNT_RANGES

PNL_CATEGORIES: tuple[str, ...] = (
    "revenue",
    "other_income",
    "cogs",
    "opex",
    "ic_charges",
    "depreciation",
)

# Output columns after the RFAREA-driven split of `opex`.
PIVOT_BUCKETS: tuple[str, ...] = (
    "revenue",
    "other_income",
    "cogs",
    "opex_production",
    "opex_rd",
    "opex_sm",
    "opex_ga",
    "opex_dist",
    "ic_charges",
    "depreciation",
)

_REVENUE_SIDE: frozenset[str] = frozenset({"revenue", "other_income"})


def pnl_category(racct: Optional[str]) -> str:
    """Return the ACCOUNT_RANGES category for a GL account string.

    Tolerates leading zeros and non-numeric inputs. Returns 'unknown' for
    empty / None / non-numeric inputs and accounts outside every defined range.
    """
    if racct is None or racct == "":
        return "unknown"
    try:
        n = int(racct)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, cat in ACCOUNT_RANGES:
        if lo <= n <= hi:
            return cat
    return "unknown"


def _categorize_column() -> Column:
    """Spark column expression mapping RACCT to its ACCOUNT_RANGES category.

    Cast-to-long is leading-zero tolerant ('0800000' -> 800000).
    """
    racct_int = F.col("RACCT").cast("long")
    expr = F.lit("unknown")
    for lo, hi, cat in ACCOUNT_RANGES:
        expr = F.when((racct_int >= lo) & (racct_int <= hi), F.lit(cat)).otherwise(expr)
    return expr


def _bucket_column() -> Column:
    """Build the pivot key: PNL_CATEGORY for everything except opex; opex is split by RFAREA."""
    return (
        F.when(F.col("PNL_CATEGORY") == "opex",
            F.when(F.col("RFAREA") == "0100", F.lit("opex_production"))
             .when(F.col("RFAREA") == "0400", F.lit("opex_rd"))
             .when(F.col("RFAREA").isin("0200", "0500"), F.lit("opex_sm"))
             .when(F.col("RFAREA") == "0600", F.lit("opex_dist"))
             .otherwise(F.lit("opex_ga"))  # 0300 + blank/null/unknown -> G&A
        ).otherwise(F.col("PNL_CATEGORY"))
    )


def build_segment_pl(acdoca_df: DataFrame, companies_df: DataFrame) -> DataFrame:
    """Aggregate ACDOCA into a segment-level P&L with functional opex split.

    Grain: (RBUKRS, ROLE_CODE, SEGMENT, GJAHR, POPER).
    Output columns: revenue, other_income, cogs, opex_production, opex_rd,
    opex_sm, opex_ga, opex_dist, ic_charges, depreciation, operating_profit,
    operating_margin.

    Filters to P&L categories only (excludes balance sheet and tax_extraordinary).
    Revenue-side amounts are sign-flipped so revenue is displayed positive.
    """
    roles = companies_df.select("RBUKRS", "ROLE_CODE")
    annotated = (
        acdoca_df
        .filter(F.col("RACCT").isNotNull())
        .withColumn("PNL_CATEGORY", _categorize_column())
        .filter(F.col("PNL_CATEGORY").isin(*PNL_CATEGORIES))
        .withColumn("BUCKET", _bucket_column())
        .join(F.broadcast(roles), "RBUKRS", "left")
    )
    pivoted = (
        annotated
        .groupBy("RBUKRS", "ROLE_CODE", "SEGMENT", "GJAHR", "POPER")
        .pivot("BUCKET", list(PIVOT_BUCKETS))
        .agg(F.sum("KSL"))
    )

    out = pivoted
    for cat in PIVOT_BUCKETS:
        col = F.coalesce(F.col(cat), F.lit(0)).cast(DecimalType(23, 2))
        if cat in _REVENUE_SIDE:
            col = -col
        out = out.withColumn(cat, col)

    out = out.withColumn(
        "operating_profit",
        (
            F.col("revenue") + F.col("other_income")
            - F.col("cogs")
            - F.col("opex_production") - F.col("opex_rd") - F.col("opex_sm")
            - F.col("opex_ga") - F.col("opex_dist")
            - F.col("ic_charges") - F.col("depreciation")
        ).cast(DecimalType(23, 2)),
    )
    rev_total = F.col("revenue") + F.col("other_income")
    out = out.withColumn(
        "operating_margin",
        F.when(rev_total != 0, F.col("operating_profit") / rev_total)
        .otherwise(F.lit(None))
        .cast(DecimalType(10, 6)),
    )
    return out
