"""Paired intercompany documents (SPEC §7.3)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL


def ic_paired_documents(
    spark: SparkSession,
    n_events: int,
    companies_indexed: DataFrame,
    gjahr: int,
    seed: int,
    group_currency: str,
    *,
    n_comp: int,
) -> DataFrame:
    """
    Each event = 2 documents x 2 lines = 4 ACDOCA rows.
    companies_indexed: ci, RBUKRS, LAND1, RHCUR, RWCUR, KOKRS, PRCTR, RCNTR, SEGMENT, FX_KSL.
    n_comp must match len(country_isos) used to build companies_indexed.
    """
    if n_events <= 0 or n_comp < 2:
        return None

    ca = companies_indexed.selectExpr(
        "ci as ci_a",
        "RBUKRS as RBUKRS_A",
        "PRCTR as PRCTR_A",
        "RCNTR as RCNTR_A",
        "LAND1 as LAND1_A",
        "RHCUR as RHCUR_A",
        "RWCUR as RWCUR_A",
        "KOKRS as KOKRS_A",
        "SEGMENT as SEGMENT_A",
        "FX_KSL as FX_KSL_A",
    )
    cb = companies_indexed.selectExpr(
        "ci as ci_b",
        "RBUKRS as RBUKRS_B",
        "PRCTR as PRCTR_B",
        "RCNTR as RCNTR_B",
        "LAND1 as LAND1_B",
        "RHCUR as RHCUR_B",
        "RWCUR as RWCUR_B",
        "KOKRS as KOKRS_B",
        "SEGMENT as SEGMENT_B",
        "FX_KSL as FX_KSL_B",
    )

    ev = spark.range(0, n_events).withColumnRenamed("id", "eid")
    ev = ev.withColumn("ci_a", F.pmod(F.hash(F.col("eid"), F.lit(seed)), F.lit(n_comp)))
    ev = ev.withColumn(
        "ci_b",
        F.pmod(
            F.col("ci_a")
            + F.lit(1)
            + F.pmod(F.hash(F.col("eid"), F.lit(seed + 99)), F.lit(max(n_comp - 1, 1))),
            F.lit(n_comp),
        ),
    )
    ev = ev.join(ca, "ci_a", "inner").join(cb, "ci_b", "inner")

    ev = ev.withColumn(
        "amt_raw",
        (F.abs(F.hash(F.col("eid"), F.lit(seed + 2))) % F.lit(800_000)) / F.lit(100.0) + F.lit(500.0),
    )
    ev = ev.withColumn("amt", F.round(F.col("amt_raw").cast(DecimalType(23, 2)), 2))
    ev = ev.withColumn(
        "AWREF",
        F.concat(
            F.lit("IC"),
            F.lpad((F.col("eid") % F.lit(1_000_000_000)).cast("string"), 9, "0"),
        ),
    )
    ev = ev.withColumn("AWTYP", F.lit("BKPF"))
    ev = ev.withColumn("AWSYS", F.lit("SYNTH"))
    ev = ev.withColumn("AWORG", F.lit("0001"))
    ev = ev.withColumn("AWITEM", F.lit("000001"))
    ev = ev.withColumn("AWITGRP", F.lit(""))

    ev = ev.withColumn("side", F.explode(F.array(F.lit("BUYER"), F.lit("SELLER"))))
    ev = ev.withColumn("ln", F.explode(F.array(F.lit(1), F.lit(2))))

    buyer = F.col("side") == F.lit("BUYER")
    ln1 = F.col("ln") == 1
    ln2 = F.col("ln") == 2

    rbukrs = F.when(buyer, F.col("RBUKRS_A")).otherwise(F.col("RBUKRS_B"))
    prctr = F.when(buyer, F.col("PRCTR_A")).otherwise(F.col("PRCTR_B"))
    rcntr = F.when(buyer, F.col("RCNTR_A")).otherwise(F.col("RCNTR_B"))
    land1 = F.when(buyer, F.col("LAND1_A")).otherwise(F.col("LAND1_B"))
    rhcur = F.when(buyer, F.col("RHCUR_A")).otherwise(F.col("RHCUR_B"))
    rwcur = F.when(buyer, F.col("RWCUR_A")).otherwise(F.col("RWCUR_B"))
    kokrs = F.when(buyer, F.col("KOKRS_A")).otherwise(F.col("KOKRS_B"))
    segment = F.when(buyer, F.col("SEGMENT_A")).otherwise(F.col("SEGMENT_B"))
    fx_ksl = F.when(buyer, F.col("FX_KSL_A")).otherwise(F.col("FX_KSL_B"))

    partner_bukrs = F.when(buyer, F.col("RBUKRS_B")).otherwise(F.col("RBUKRS_A"))
    partner_prctr = F.when(buyer, F.col("PRCTR_B")).otherwise(F.col("PRCTR_A"))

    seller = F.col("side") == F.lit("SELLER")

    racct = (
        F.when(buyer & ln1, F.lit(SAMPLE_GL["inventory_fg"]))
        .when(buyer & ln2, F.lit(SAMPLE_GL["ap_ic"]))
        .when(seller & ln1, F.lit(SAMPLE_GL["ar_ic"]))
        .otherwise(F.lit(SAMPLE_GL["revenue_ic"]))
    )

    wsl = (
        F.when(buyer & ln1, F.col("amt"))
        .when(buyer & ln2, -F.col("amt"))
        .when(seller & ln1, F.col("amt"))
        .otherwise(-F.col("amt"))
    )

    bschl = F.when(ln1, F.lit("40")).otherwise(F.lit("50"))
    drcrk = F.when(ln1, F.lit("S")).otherwise(F.lit("H"))
    docln = F.when(ln1, F.lit("000001")).otherwise(F.lit("000002"))
    buzei = F.when(ln1, F.lit("001")).otherwise(F.lit("002"))

    poper_i = (F.pmod(F.hash(F.col("eid"), F.lit(seed + 5)), F.lit(12)) + F.lit(1)).cast("int")
    belnr_num = F.lit(8_000_000_000) + F.col("eid") * F.lit(2) + F.when(buyer, F.lit(0)).otherwise(F.lit(1))
    belnr = F.lpad((F.pmod(belnr_num, F.lit(10_000_000_000))).cast("string"), 10, "0")

    rfarea = (
        F.when(racct == F.lit(SAMPLE_GL["revenue_ic"]), F.lit("0200"))
        .when(racct == F.lit(SAMPLE_GL["cogs_ic"]), F.lit("0100"))
        .when(racct == F.lit(SAMPLE_GL["ic_service_exp"]), F.lit("0300"))
        .otherwise(F.lit(""))  # BS legs (inventory_fg, ap_ic, ar_ic) blank per SAP
    )

    out = (
        ev.withColumn("RBUKRS", rbukrs)
        .withColumn("PRCTR", prctr)
        .withColumn("RCNTR", rcntr)
        .withColumn("LAND1", land1)
        .withColumn("RHCUR", rhcur)
        .withColumn("RWCUR", rwcur)
        .withColumn("KOKRS", kokrs)
        .withColumn("SEGMENT", segment)
        .withColumn("FX_KSL", fx_ksl)
        .withColumn("RASSC", partner_bukrs)
        .withColumn("PPRCTR", partner_prctr)
        .withColumn("PBUKRS", partner_bukrs)
        .withColumn("RACCT", racct)
        .withColumn("RFAREA", rfarea)
        .withColumn("WSL", wsl.cast(DecimalType(23, 2)))
        .withColumn("BSCHL", bschl)
        .withColumn("DRCRK", drcrk)
        .withColumn("DOCLN", docln)
        .withColumn("BUZEI", buzei)
        .withColumn("poper_i", poper_i)
        .withColumn("GJAHR", F.lit(gjahr))
        .withColumn("RYEAR", F.lit(gjahr))
        .withColumn("POPER", F.lpad(F.col("poper_i").cast("string"), 3, "0"))
        .withColumn("BUDAT", F.make_date(F.lit(gjahr), F.col("poper_i"), F.lit(20)))
        .withColumn("BLDAT", F.col("BUDAT"))
        .withColumn("FISCYEARPER", F.col("GJAHR") * F.lit(1000) + F.col("poper_i"))
        .withColumn("PERIV", F.lit("K4"))
        .withColumn("BLART", F.lit("AB"))
        .withColumn("BSTAT", F.lit(" "))
        .withColumn("RCLNT", F.lit("100"))
        .withColumn("RLDNR", F.lit("0L"))
        .withColumn("RRCTY", F.lit("0"))
        .withColumn("RMVCT", F.lit(""))
        .withColumn("RKCUR", F.lit(group_currency))
        .withColumn("HSL", F.col("WSL").cast(DecimalType(23, 2)))
        .withColumn("KSL", F.round(F.col("WSL") * F.col("FX_KSL"), 2).cast(DecimalType(23, 2)))
        .withColumn("BELNR", belnr)
        .withColumn("DOCNR_LD", F.col("BELNR"))
        .withColumn("KOART", F.lit("S"))
        .withColumn("GLACCOUNT_TYPE", F.lit("P"))
        .withColumn("KTOPL", F.lit("INTL"))
        .withColumn("USNAM", F.lit("SYNTH_IC"))
        .withColumn("TIMESTAMP", F.to_timestamp(F.col("eid") + F.col("ln") + F.lit(seed * 1_000_000)))
        .withColumn("SGTXT", F.lit("Synthetic IC posting"))
        .withColumn("TAX_COUNTRY", F.col("LAND1"))
        .withColumn("LIFNR", F.lit(None).cast(StringType()))
        .withColumn("KUNNR", F.lit(None).cast(StringType()))
        .withColumn("RUNIT", F.lit(None).cast(StringType()))
        .withColumn("MSL", F.lit(None).cast(DecimalType(23, 3)))
        .withColumn("XREVERSING", F.lit(""))
        .withColumn("XREVERSED", F.lit(""))
        .withColumn("XTRUEREV", F.lit(""))
    )

    drop_cols = [
        "ci_a",
        "ci_b",
        "RBUKRS_A",
        "RBUKRS_B",
        "PRCTR_A",
        "PRCTR_B",
        "RCNTR_A",
        "RCNTR_B",
        "LAND1_A",
        "LAND1_B",
        "RHCUR_A",
        "RHCUR_B",
        "RWCUR_A",
        "RWCUR_B",
        "KOKRS_A",
        "KOKRS_B",
        "SEGMENT_A",
        "SEGMENT_B",
        "FX_KSL_A",
        "FX_KSL_B",
        "amt_raw",
        "amt",
        "eid",
        "side",
        "ln",
        "poper_i",
        "FX_KSL",
    ]
    for c in drop_cols:
        if c in out.columns:
            out = out.drop(c)
    return out
