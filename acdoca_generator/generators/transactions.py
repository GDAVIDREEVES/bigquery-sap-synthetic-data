"""Domestic (single-entity) journal lines (SPEC §7)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DecimalType, StringType

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.industries import IndustryTemplate

# Balanced two-line postings: (debit account key, credit account key) in SAMPLE_GL.
MIX_GL_PAIRS: dict[str, tuple[str, str]] = {
    "third_party_revenue": ("ar_trade", "revenue_tp"),
    "procurement_cogs": ("cogs", "ap_trade"),
    "ic_goods": ("cogs_ic", "ap_ic"),
    "ic_services": ("ic_service_exp", "ap_ic"),
    "payroll": ("opex_sga", "cash"),
    "rd": ("opex_rd", "cash"),
    "depreciation": ("opex_sga", "ap_trade"),
    "tax": ("opex_sga", "ap_trade"),
    "other": ("opex_sga", "cash"),
}


def _amount_multipliers(industry: IndustryTemplate) -> dict[str, float]:
    m = (industry.gross_margin_low + industry.gross_margin_high) / 2.0
    return {
        "third_party_revenue": 1.1 + m,
        "procurement_cogs": 0.45 + (1.0 - m) * 0.6,
        "ic_goods": 0.55 + (1.0 - m) * 0.4,
        "ic_services": 0.7 + (1.0 - m) * 0.3,
        "payroll": 0.85,
        "rd": 0.9,
        "depreciation": 0.5,
        "tax": 0.55,
        "other": 0.8,
    }


def _cumulative_bands(weights: dict[str, float]) -> list[tuple[str, float]]:
    keys = sorted(weights.keys())
    s = sum(weights[k] for k in keys)
    if s <= 0:
        return [(keys[0], 1.0)]
    cum = 0.0
    bands: list[tuple[str, float]] = []
    for k in keys:
        cum += weights[k] / s
        bands.append((k, cum))
    return bands


def _pick_from_bands(u_col: Column, bands: list[tuple[str, float]]) -> Column:
    expr = F.when(u_col < F.lit(bands[0][1]), F.lit(bands[0][0]))
    for k, hi in bands[1:]:
        expr = expr.when(u_col < F.lit(hi), F.lit(k))
    return expr.otherwise(F.lit(bands[-1][0]))


def _seasonality_bands(seasonality_month: dict[int, float]) -> list[tuple[int, float]]:
    months = list(range(1, 13))
    w = [float(seasonality_month.get(m, 1.0)) for m in months]
    s = sum(w)
    if s <= 0:
        return [(1, 1.0)]
    cum = 0.0
    bands: list[tuple[int, float]] = []
    for m, wi in zip(months, w):
        cum += wi / s
        bands.append((m, cum))
    return bands


def _poper_from_seasonality(doc_id_col: Column, seed: int, industry: IndustryTemplate) -> Column:
    u = (
        F.pmod(F.hash(doc_id_col, F.lit(seed + 3)), F.lit(1_000_000_000)).cast("double")
        / F.lit(1_000_000_000.0)
    )
    bands = _seasonality_bands(industry.seasonality_month)
    expr = F.when(u < F.lit(bands[0][1]), F.lit(bands[0][0]))
    for m, hi in bands[1:]:
        expr = expr.when(u < F.lit(hi), F.lit(m))
    return expr.otherwise(F.lit(bands[-1][0])).cast("int")


def _gl_pair_columns(txn_type: Column) -> tuple[Column, Column]:
    debit = None
    credit = None
    for txn, (dk, ck) in MIX_GL_PAIRS.items():
        d_lit = F.lit(SAMPLE_GL[dk])
        c_lit = F.lit(SAMPLE_GL[ck])
        if debit is None:
            debit = F.when(txn_type == F.lit(txn), d_lit)
            credit = F.when(txn_type == F.lit(txn), c_lit)
        else:
            debit = debit.when(txn_type == F.lit(txn), d_lit)
            credit = credit.when(txn_type == F.lit(txn), c_lit)
    return (
        debit.otherwise(F.lit(SAMPLE_GL["opex_sga"])),
        credit.otherwise(F.lit(SAMPLE_GL["cash"])),
    )


def _amt_scale_column(txn_type: Column, industry: IndustryTemplate) -> Column:
    mults = _amount_multipliers(industry)
    expr = None
    for txn, mult in sorted(mults.items()):
        if expr is None:
            expr = F.when(txn_type == F.lit(txn), F.lit(mult))
        else:
            expr = expr.when(txn_type == F.lit(txn), F.lit(mult))
    return expr.otherwise(F.lit(0.85))


def domestic_balanced_documents(
    spark: SparkSession,
    n_docs: int,
    companies_indexed: DataFrame,
    gjahr: int,
    seed: int,
    group_currency: str,
    include_reversals: bool,
    industry: IndustryTemplate,
) -> DataFrame:
    """
    n_docs two-line GL documents, balanced on WSL.
    companies_indexed must include RBUKRS, LAND1, RHCUR, RWCUR, KOKRS, PRCTR, RCNTR, SEGMENT, FX_KSL.
    """
    if n_docs <= 0:
        return None

    n_row = companies_indexed.select(F.countDistinct("ci").alias("n")).collect()[0].n
    n_comp = int(n_row or 0)
    if n_comp == 0:
        return None

    base = spark.range(0, n_docs).withColumnRenamed("id", "doc_id")
    base = base.withColumn("ci", F.pmod(F.hash(F.col("doc_id"), F.lit(seed)), F.lit(n_comp)))
    base = base.join(companies_indexed, "ci", "inner").drop("ci")

    u_txn = (
        F.pmod(F.hash(F.col("doc_id"), F.lit(seed + 99)), F.lit(1_000_000_000)).cast("double")
        / F.lit(1_000_000_000.0)
    )
    base = base.withColumn("u_txn", u_txn)
    mix_bands = _cumulative_bands(industry.normalized_mix())
    base = base.withColumn("txn_type", _pick_from_bands(F.col("u_txn"), mix_bands))
    base = base.drop("u_txn")

    base = base.withColumn(
        "amt_raw",
        (F.abs(F.hash(F.col("doc_id"), F.lit(seed + 1))) % F.lit(500_000)) / F.lit(100.0) + F.lit(100.0),
    )
    base = base.withColumn(
        "amt",
        F.round(
            F.col("amt_raw").cast("double") * _amt_scale_column(F.col("txn_type"), industry),
            2,
        ).cast(DecimalType(23, 2)),
    )

    base = base.withColumn("lines", F.array(F.lit(1), F.lit(2)))
    base = base.withColumn("ln", F.explode("lines")).drop("lines")

    debit_gl, credit_gl = _gl_pair_columns(F.col("txn_type"))
    racct = F.when(F.col("ln") == 1, debit_gl).otherwise(credit_gl)
    bschl = F.when(F.col("ln") == 1, F.lit("40")).otherwise(F.lit("50"))
    drcrk = F.when(F.col("ln") == 1, F.lit("S")).otherwise(F.lit("H"))
    wsl = F.when(F.col("ln") == 1, F.col("amt")).otherwise(-F.col("amt"))
    docln = F.when(F.col("ln") == 1, F.lit("000001")).otherwise(F.lit("000002"))
    buzei = F.when(F.col("ln") == 1, F.lit("001")).otherwise(F.lit("002"))

    base = base.withColumn("poper_i", _poper_from_seasonality(F.col("doc_id"), seed, industry))
    base = base.withColumn("POPER", F.lpad(F.col("poper_i").cast("string"), 3, "0"))
    base = base.withColumn("GJAHR", F.lit(gjahr))
    base = base.withColumn("RYEAR", F.lit(gjahr))
    base = base.withColumn("BUDAT", F.make_date(F.lit(gjahr), F.col("poper_i"), F.lit(15)))
    base = base.withColumn("BLDAT", F.col("BUDAT"))
    base = base.withColumn("FISCYEARPER", F.col("GJAHR") * F.lit(1000) + F.col("poper_i"))
    base = base.withColumn("PERIV", F.lit("K4"))
    base = base.withColumn("BLART", F.lit("SA"))
    base = base.withColumn("BSTAT", F.lit(" "))
    base = base.withColumn("RCLNT", F.lit("100"))
    base = base.withColumn("RLDNR", F.lit("0L"))
    base = base.withColumn("RRCTY", F.lit("0"))
    base = base.withColumn("RMVCT", F.lit(""))
    base = base.withColumn("RACCT", racct)
    base = base.withColumn("BSCHL", bschl)
    base = base.withColumn("DRCRK", drcrk)
    base = base.withColumn("WSL", wsl.cast(DecimalType(23, 2)))
    base = base.withColumn("RWCUR", F.col("RWCUR"))
    base = base.withColumn("RHCUR", F.col("RHCUR"))
    base = base.withColumn("RKCUR", F.lit(group_currency))
    base = base.withColumn("HSL", F.col("WSL").cast(DecimalType(23, 2)))
    base = base.withColumn("KSL", F.round(F.col("WSL") * F.col("FX_KSL"), 2).cast(DecimalType(23, 2)))
    base = base.withColumn("DOCLN", docln)
    base = base.withColumn("BUZEI", buzei)
    base = base.withColumn(
        "BELNR",
        F.lpad((F.col("doc_id") % F.lit(10_000_000_000)).cast("string"), 10, "0"),
    )
    base = base.withColumn("DOCNR_LD", F.col("BELNR"))
    base = base.withColumn("KOART", F.lit("S"))
    base = base.withColumn("GLACCOUNT_TYPE", F.lit("P"))
    base = base.withColumn("KTOPL", F.lit("INTL"))
    base = base.withColumn("USNAM", F.lit("SYNTH_USER"))
    base = base.withColumn("TIMESTAMP", F.to_timestamp(F.col("doc_id") + F.lit(seed * 1_000_000)))
    base = base.withColumn("SGTXT", F.lit("Synthetic domestic posting"))
    base = base.withColumn("LAND1", F.col("LAND1"))
    base = base.withColumn("TAX_COUNTRY", F.col("LAND1"))
    base = base.withColumn("RASSC", F.lit(""))
    base = base.withColumn("PPRCTR", F.col("PRCTR"))
    base = base.withColumn("PBUKRS", F.col("RBUKRS"))
    base = base.withColumn("LIFNR", F.lit(""))
    base = base.withColumn("KUNNR", F.lit(""))
    base = base.withColumn("RUNIT", F.lit(None).cast(StringType()))
    base = base.withColumn("MSL", F.lit(None).cast(DecimalType(23, 3)))

    if include_reversals:
        rev_flag = F.when(
            F.pmod(F.hash(F.col("doc_id")), F.lit(100)) < F.lit(5),
            F.lit("X"),
        ).otherwise(F.lit(""))
    else:
        rev_flag = F.lit("")
    base = base.withColumn("XREVERSING", rev_flag)
    base = base.withColumn("XREVERSED", F.lit(""))
    base = base.withColumn("XTRUEREV", F.lit(""))

    drop_cols = ["doc_id", "ln", "amt", "amt_raw", "poper_i", "txn_type", "FX_KSL"]
    for c in drop_cols:
        if c in base.columns:
            base = base.drop(c)
    return base
