"""Domestic (single-entity) journal lines (SPEC §7)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL


def domestic_balanced_documents(
    spark: SparkSession,
    n_docs: int,
    companies_indexed: DataFrame,
    gjahr: int,
    seed: int,
    group_currency: str,
    include_reversals: bool,
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

    base = base.withColumn(
        "amt_raw",
        (F.abs(F.hash(F.col("doc_id"), F.lit(seed + 1))) % F.lit(500_000)) / F.lit(100.0) + F.lit(100.0),
    )
    base = base.withColumn("amt", F.round(F.col("amt_raw").cast(DecimalType(23, 2)), 2))

    base = base.withColumn("lines", F.array(F.lit(1), F.lit(2)))
    base = base.withColumn("ln", F.explode("lines")).drop("lines")

    debit_acct = F.lit(SAMPLE_GL["opex_sga"])
    credit_acct = F.lit(SAMPLE_GL["cash"])
    racct = F.when(F.col("ln") == 1, debit_acct).otherwise(credit_acct)
    bschl = F.when(F.col("ln") == 1, F.lit("40")).otherwise(F.lit("50"))
    drcrk = F.when(F.col("ln") == 1, F.lit("S")).otherwise(F.lit("H"))
    wsl = F.when(F.col("ln") == 1, F.col("amt")).otherwise(-F.col("amt"))
    docln = F.when(F.col("ln") == 1, F.lit("000001")).otherwise(F.lit("000002"))
    buzei = F.when(F.col("ln") == 1, F.lit("001")).otherwise(F.lit("002"))

    poper_i = (F.pmod(F.hash(F.col("doc_id"), F.lit(seed + 3)), F.lit(12)) + F.lit(1)).cast("int")
    base = base.withColumn("poper_i", poper_i)
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

    drop_cols = ["doc_id", "ln", "amt", "amt_raw", "poper_i", "FX_KSL"]
    for c in drop_cols:
        if c in base.columns:
            base = base.drop(c)
    return base
