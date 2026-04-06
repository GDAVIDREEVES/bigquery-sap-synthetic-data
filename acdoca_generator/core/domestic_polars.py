"""Domestic balanced documents in Polars (fast path; hashing differs from Spark SQL hash)."""

from __future__ import annotations

import polars as pl

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.industries import IndustryTemplate
from acdoca_generator.core.companies_polars import build_companies_indexed_polars
from acdoca_generator.generators.transactions import (
    MIX_GL_PAIRS,
    _amount_multipliers,
    _cumulative_bands,
    _seasonality_bands,
)


def _polars_pick_str_bands(u: pl.Expr, bands: list[tuple[str, float]]) -> pl.Expr:
    expr = pl.when(u < pl.lit(bands[0][1])).then(pl.lit(bands[0][0]))
    for k, hi in bands[1:]:
        expr = expr.when(u < pl.lit(hi)).then(pl.lit(k))
    return expr.otherwise(pl.lit(bands[-1][0]))


def _polars_pick_int_bands(u: pl.Expr, bands: list[tuple[int, float]]) -> pl.Expr:
    expr = pl.when(u < pl.lit(bands[0][1])).then(pl.lit(bands[0][0]))
    for m, hi in bands[1:]:
        expr = expr.when(u < pl.lit(hi)).then(pl.lit(m))
    return expr.otherwise(pl.lit(bands[-1][0]))


def _poper_i_expr(doc_id: pl.Expr, seed: int, industry: IndustryTemplate) -> pl.Expr:
    u = (doc_id.hash(seed=seed + 3) % pl.lit(1_000_000_000)).cast(pl.Float64) / pl.lit(1_000_000_000.0)
    bands = _seasonality_bands(industry.seasonality_month)
    return _polars_pick_int_bands(u, bands).cast(pl.Int32)


def _amt_scale_expr(txn_type: pl.Expr, industry: IndustryTemplate) -> pl.Expr:
    mults = _amount_multipliers(industry)
    expr: pl.Expr | None = None
    for txn, mult in sorted(mults.items()):
        if expr is None:
            expr = pl.when(txn_type == pl.lit(txn)).then(pl.lit(mult))
        else:
            expr = expr.when(txn_type == pl.lit(txn)).then(pl.lit(mult))
    assert expr is not None
    return expr.otherwise(pl.lit(0.85))


def _racct_expr(txn_type: pl.Expr, ln: pl.Expr) -> pl.Expr:
    debit: pl.Expr | None = None
    credit: pl.Expr | None = None
    for txn, (dk, ck) in MIX_GL_PAIRS.items():
        d_lit = pl.lit(SAMPLE_GL[dk])
        c_lit = pl.lit(SAMPLE_GL[ck])
        if debit is None:
            debit = pl.when(txn_type == pl.lit(txn)).then(d_lit)
            credit = pl.when(txn_type == pl.lit(txn)).then(c_lit)
        else:
            debit = debit.when(txn_type == pl.lit(txn)).then(d_lit)
            credit = credit.when(txn_type == pl.lit(txn)).then(c_lit)
    assert debit is not None and credit is not None
    debit = debit.otherwise(pl.lit(SAMPLE_GL["opex_sga"]))
    credit = credit.otherwise(pl.lit(SAMPLE_GL["cash"]))
    return pl.when(ln == 1).then(debit).otherwise(credit)


def domestic_balanced_polars(
    n_docs: int,
    *,
    country_isos: list[str],
    industry_key: str,
    gjahr: int,
    seed: int,
    group_currency: str,
    include_reversals: bool,
    industry: IndustryTemplate,
) -> pl.DataFrame | None:
    """
    Same shape and invariants as Spark ``domestic_balanced_documents`` (balanced WSL per doc).
    Uses Polars row hashing (not Spark SQL ``hash()``), so row-level values differ from Spark.
    """
    if n_docs <= 0:
        return None
    comp = build_companies_indexed_polars(country_isos, industry_key, seed, group_currency)
    n_comp = comp.height
    if n_comp <= 0:
        return None

    doc_id = pl.int_range(0, n_docs, dtype=pl.Int64, eager=True).alias("doc_id")
    base = pl.DataFrame({"doc_id": doc_id})
    base = base.with_columns(
        (pl.col("doc_id").hash(seed=seed) % pl.lit(n_comp)).cast(pl.Int64).alias("ci")
    )
    base = base.join(comp, on="ci", how="inner")

    u_txn = (
        (pl.col("doc_id").hash(seed=seed + 99) % pl.lit(1_000_000_000)).cast(pl.Float64)
        / pl.lit(1_000_000_000.0)
    )
    mix_bands = _cumulative_bands(industry.normalized_mix())
    base = base.with_columns(
        u_txn.alias("u_txn"),
        _polars_pick_str_bands(u_txn, mix_bands).alias("txn_type"),
    ).drop("u_txn")

    base = base.with_columns(
        (
            (pl.col("doc_id").hash(seed=seed + 1) % pl.lit(500_000)).cast(pl.Float64) / pl.lit(100.0)
            + pl.lit(100.0)
        ).alias("amt_raw")
    )
    base = base.with_columns(
        (
            pl.col("amt_raw")
            * _amt_scale_expr(pl.col("txn_type"), industry).cast(pl.Float64)
        )
        .round(2)
        .cast(pl.Decimal(precision=23, scale=2))
        .alias("amt")
    )

    b1 = base.with_columns(pl.lit(1).alias("ln"))
    b2 = base.with_columns(pl.lit(2).alias("ln"))
    base = pl.concat([b1, b2], how="vertical").sort("doc_id", "ln")

    ln = pl.col("ln")
    racct = _racct_expr(pl.col("txn_type"), ln)
    bschl = pl.when(ln == 1).then(pl.lit("40")).otherwise(pl.lit("50"))
    drcrk = pl.when(ln == 1).then(pl.lit("S")).otherwise(pl.lit("H"))
    wsl = pl.when(ln == 1).then(pl.col("amt")).otherwise(-pl.col("amt"))
    docln = pl.when(ln == 1).then(pl.lit("000001")).otherwise(pl.lit("000002"))
    buzei = pl.when(ln == 1).then(pl.lit("001")).otherwise(pl.lit("002"))

    poper_i = _poper_i_expr(pl.col("doc_id"), seed, industry)
    belnr = (
        (pl.col("doc_id") % pl.lit(10_000_000_000))
        .cast(pl.Utf8)
        .str.zfill(10)
    )

    if include_reversals:
        rev = pl.when((pl.col("doc_id").hash(seed=0) % pl.lit(100)) < pl.lit(5)).then(pl.lit("X")).otherwise(
            pl.lit("")
        )
    else:
        rev = pl.lit("")

    ts_epoch = (pl.col("doc_id") + pl.lit(seed * 1_000_000)).cast(pl.Int64)
    ts = pl.from_epoch(ts_epoch, time_unit="s")

    base = base.with_columns(poper_i.alias("poper_i"))
    budat = pl.datetime(pl.lit(gjahr), pl.col("poper_i"), pl.lit(15), time_unit="us")
    base = base.with_columns(
        [
            pl.col("poper_i").cast(pl.Utf8).str.zfill(3).alias("POPER"),
            pl.lit(gjahr).alias("GJAHR"),
            pl.lit(gjahr).alias("RYEAR"),
            budat.alias("BUDAT"),
            pl.lit("K4").alias("PERIV"),
            pl.lit("SA").alias("BLART"),
            pl.lit(" ").alias("BSTAT"),
            pl.lit("100").alias("RCLNT"),
            pl.lit("0L").alias("RLDNR"),
            pl.lit("0").alias("RRCTY"),
            pl.lit("").alias("RMVCT"),
            racct.alias("RACCT"),
            bschl.alias("BSCHL"),
            drcrk.alias("DRCRK"),
            wsl.cast(pl.Decimal(precision=23, scale=2)).alias("WSL"),
            pl.col("RWCUR"),
            pl.col("RHCUR"),
            pl.lit(group_currency).alias("RKCUR"),
            wsl.cast(pl.Decimal(precision=23, scale=2)).alias("HSL"),
            docln.alias("DOCLN"),
            buzei.alias("BUZEI"),
            belnr.alias("BELNR"),
            belnr.alias("DOCNR_LD"),
            pl.lit("S").alias("KOART"),
            pl.lit("P").alias("GLACCOUNT_TYPE"),
            pl.lit("INTL").alias("KTOPL"),
            pl.lit("SYNTH_USER").alias("USNAM"),
            ts.alias("TIMESTAMP"),
            pl.lit("Synthetic domestic posting").alias("SGTXT"),
            pl.col("LAND1"),
            pl.col("LAND1").alias("TAX_COUNTRY"),
            pl.lit("").alias("RASSC"),
            pl.col("PRCTR").alias("PPRCTR"),
            pl.col("RBUKRS").alias("PBUKRS"),
            pl.lit("").alias("LIFNR"),
            pl.lit("").alias("KUNNR"),
            pl.lit(None).cast(pl.Utf8).alias("RUNIT"),
            pl.lit(None).cast(pl.Decimal(precision=23, scale=3)).alias("MSL"),
            rev.alias("XREVERSING"),
            pl.lit("").alias("XREVERSED"),
            pl.lit("").alias("XTRUEREV"),
        ]
    )

    base = base.with_columns(
        (pl.col("WSL").cast(pl.Float64) * pl.col("FX_KSL"))
        .round(2)
        .cast(pl.Decimal(precision=23, scale=2))
        .alias("KSL"),
    )

    base = base.with_columns(
        (pl.col("GJAHR") * pl.lit(1000) + pl.col("poper_i").cast(pl.Int64)).alias("FISCYEARPER"),
        pl.col("BUDAT").alias("BLDAT"),
    )

    drop_cols = ["doc_id", "ln", "amt", "amt_raw", "poper_i", "txn_type", "FX_KSL", "ci", "ROLE_CODE"]
    return base.drop(*[c for c in drop_cols if c in base.columns])
