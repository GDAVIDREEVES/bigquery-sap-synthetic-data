"""Period-end style closing postings (SPEC §7.4)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType, StructField, StructType

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL

_CLOSING_STEPS = ("010", "020", "030", "040", "050", "060", "070")


def closing_balanced_documents(
    spark: SparkSession,
    companies_indexed: DataFrame,
    gjahr: int,
    seed: int,
    group_currency: str,
) -> DataFrame:
    """
    One balanced two-line document per (company, period, closing step).
    """
    n_comp = int(companies_indexed.select(F.countDistinct("ci").alias("n")).collect()[0].n or 0)
    if n_comp == 0:
        return None

    rows: list[tuple[int, int, str, int]] = []
    doc_id = 0
    for ci in range(n_comp):
        for step in _CLOSING_STEPS:
            for poper_i in range(1, 13):
                rows.append((doc_id, ci, step, poper_i))
                doc_id += 1

    base_schema = StructType(
        [
            StructField("doc_id", LongType(), False),
            StructField("ci", LongType(), False),
            StructField("CLOSINGSTEP", StringType(), False),
            StructField("poper_i", IntegerType(), False),
        ]
    )
    base = spark.createDataFrame(rows, base_schema)
    base = base.join(companies_indexed, "ci", "inner")

    base = base.withColumn(
        "amt",
        F.round(
            (
                (F.abs(F.hash(F.col("doc_id"), F.lit(seed + 7))) % F.lit(200_000)) / F.lit(100.0)
                + F.lit(50.0)
            ).cast(DecimalType(23, 2)),
            2,
        ),
    )
    base = base.withColumn("lines", F.array(F.lit(1), F.lit(2)))
    base = base.withColumn("ln", F.explode("lines")).drop("lines")

    ln1 = F.col("ln") == 1
    racct = F.when(ln1, F.lit(SAMPLE_GL["retained_earnings"])).otherwise(F.lit(SAMPLE_GL["opex_sga"]))
    wsl = F.when(ln1, F.col("amt")).otherwise(-F.col("amt"))
    drcrk = F.when(ln1, F.lit("S")).otherwise(F.lit("H"))
    bschl = F.when(ln1, F.lit("40")).otherwise(F.lit("50"))
    docln = F.when(ln1, F.lit("000001")).otherwise(F.lit("000002"))
    buzei = F.when(ln1, F.lit("001")).otherwise(F.lit("002"))

    out = (
        base.withColumn("GJAHR", F.lit(gjahr))
        .withColumn("RYEAR", F.lit(gjahr))
        .withColumn("POPER", F.lpad(F.col("poper_i").cast("string"), 3, "0"))
        .withColumn("BUDAT", F.make_date(F.lit(gjahr), F.col("poper_i"), F.lit(28)))
        .withColumn("BLDAT", F.col("BUDAT"))
        .withColumn("FISCYEARPER", F.col("GJAHR") * F.lit(1000) + F.col("poper_i"))
        .withColumn("PERIV", F.lit("K4"))
        .withColumn("BLART", F.lit("SA"))
        .withColumn("BSTAT", F.lit(" "))
        .withColumn("RCLNT", F.lit("100"))
        .withColumn("RLDNR", F.lit("0L"))
        .withColumn("RRCTY", F.lit("0"))
        .withColumn("RMVCT", F.lit(""))
        .withColumn("RACCT", racct)
        .withColumn("WSL", wsl.cast(DecimalType(23, 2)))
        .withColumn("BSCHL", bschl)
        .withColumn("DRCRK", drcrk)
        .withColumn("DOCLN", docln)
        .withColumn("BUZEI", buzei)
        .withColumn("RWCUR", F.col("RWCUR"))
        .withColumn("RHCUR", F.col("RHCUR"))
        .withColumn("RKCUR", F.lit(group_currency))
        .withColumn("HSL", F.col("WSL").cast(DecimalType(23, 2)))
        .withColumn("KSL", F.round(F.col("WSL") * F.col("FX_KSL"), 2).cast(DecimalType(23, 2)))
        .withColumn(
            "BELNR",
            F.lpad(
                (F.lit(6_000_000_000) + F.col("doc_id") % F.lit(3_000_000_000)).cast("string"),
                10,
                "0",
            ),
        )
        .withColumn("DOCNR_LD", F.col("BELNR"))
        .withColumn("KOART", F.lit("S"))
        .withColumn("GLACCOUNT_TYPE", F.lit("P"))
        .withColumn("KTOPL", F.lit("INTL"))
        .withColumn("USNAM", F.lit("SYNTH_CLOSE"))
        .withColumn("TIMESTAMP", F.to_timestamp(F.col("doc_id") + F.lit(seed * 2_000_000)))
        .withColumn("SGTXT", F.concat(F.lit("Closing "), F.col("CLOSINGSTEP")))
        .withColumn("LAND1", F.col("LAND1"))
        .withColumn("TAX_COUNTRY", F.col("LAND1"))
        .withColumn("RASSC", F.lit(""))
        .withColumn("PPRCTR", F.col("PRCTR"))
        .withColumn("PBUKRS", F.col("RBUKRS"))
        .withColumn("LIFNR", F.lit(None).cast(StringType()))
        .withColumn("KUNNR", F.lit(None).cast(StringType()))
        .withColumn("RUNIT", F.lit(None).cast(StringType()))
        .withColumn("MSL", F.lit(None).cast(DecimalType(23, 3)))
        .withColumn("XREVERSING", F.lit(""))
        .withColumn("XREVERSED", F.lit(""))
        .withColumn("XTRUEREV", F.lit(""))
        .withColumn("CLOSING_RUN_ID", F.concat(F.lit("RUN"), F.col("CLOSINGSTEP")))
    )

    for c in ("doc_id", "ci", "ln", "amt", "poper_i", "FX_KSL"):
        if c in out.columns:
            out = out.drop(c)
    return out
