"""End-to-end ACDOCA generation (SPEC §2, §10)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from acdoca_generator.config.field_tiers import excluded_sql_names, fields_for_complexity
from acdoca_generator.config.industries import get_industry
from acdoca_generator.generators.amounts import fx_multiplier
from acdoca_generator.generators.closing import closing_balanced_documents
from acdoca_generator.generators.intercompany import ic_paired_documents
from acdoca_generator.generators.master_data import build_companies
from acdoca_generator.generators.supply_chain import (
    export_supply_chain_flows_json,
    generate_supply_chain_flows,
)
from acdoca_generator.generators.transactions import domestic_balanced_documents
from acdoca_generator.utils.schema import acdoca_schema


@dataclass
class GenerationConfig:
    industry_key: str
    country_isos: List[str]
    fiscal_year: int
    fiscal_variant: str  # "calendar" | "april"
    complexity: str
    txn_per_cc_per_period: int
    include_reversals: bool
    include_closing: bool
    seed: int
    group_currency: str = "USD"
    ic_pct: Optional[float] = None  # None → industry template ic_share_default
    include_supply_chain: bool = False
    sc_chains_per_period: int = 50


@dataclass
class GenerationResult:
    acdoca_df: DataFrame
    supply_chain_flows_df: Optional[DataFrame] = None


def _companies_indexed_with_fx(
    spark: SparkSession, companies_df: DataFrame, group_currency: str, seed: int
) -> DataFrame:
    w = Window.orderBy("RBUKRS")
    idx = companies_df.withColumn("ci", F.row_number().over(w) - F.lit(1))
    rows = idx.collect()
    fx_rows = [(r.RBUKRS, float(fx_multiplier(r.RHCUR, group_currency, seed, 1))) for r in rows]
    fx_df = spark.createDataFrame(fx_rows, ["RBUKRS", "FX_KSL"])
    return idx.join(fx_df, "RBUKRS", "left")


def _align_to_schema(df: DataFrame) -> DataFrame:
    schema = acdoca_schema()
    out = df
    for field in schema.fields:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
    return out.select(*[f.name for f in schema.fields])


def _null_fields_above_tier(df: DataFrame, complexity: str) -> DataFrame:
    populated = fields_for_complexity(complexity)
    excl = excluded_sql_names()
    schema = acdoca_schema()
    out = df
    for field in schema.fields:
        if field.name in excl or field.name not in populated:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
    return out


def _fill_tier_defaults(df: DataFrame, complexity: str) -> DataFrame:
    """Populate optional reference fields when tier allows (coverage / demos)."""
    pop = fields_for_complexity(complexity)
    out = df
    if "VORGN" in pop:
        out = out.withColumn("VORGN", F.coalesce(F.col("VORGN"), F.lit("RFBU")))
    if "VRGNG" in pop:
        out = out.withColumn("VRGNG", F.coalesce(F.col("VRGNG"), F.lit("RFBU")))
    if "BTTYPE" in pop:
        out = out.withColumn("BTTYPE", F.coalesce(F.col("BTTYPE"), F.lit("")))
    if "LINETYPE" in pop:
        out = out.withColumn("LINETYPE", F.coalesce(F.col("LINETYPE"), F.lit("")))
    if "KTOSL" in pop:
        out = out.withColumn("KTOSL", F.coalesce(F.col("KTOSL"), F.lit("")))
    if "MWSKZ" in pop:
        out = out.withColumn("MWSKZ", F.coalesce(F.col("MWSKZ"), F.lit("V0")))
    if "NETDT" in pop:
        out = out.withColumn("NETDT", F.coalesce(F.col("NETDT"), F.col("BUDAT")))
    if "VALUT" in pop:
        out = out.withColumn("VALUT", F.coalesce(F.col("VALUT"), F.col("BUDAT")))
    if "SDM_VERSION" in pop:
        out = out.withColumn("SDM_VERSION", F.coalesce(F.col("SDM_VERSION"), F.lit("1")))
    if "RFCCUR" in pop:
        out = out.withColumn("RFCCUR", F.coalesce(F.col("RFCCUR"), F.col("RHCUR")))
    if "FCSL" in pop:
        out = out.withColumn(
            "FCSL",
            F.coalesce(F.col("FCSL"), F.col("HSL").cast(DecimalType(23, 2))),
        )
    if "RBUNIT" in pop:
        out = out.withColumn("RBUNIT", F.coalesce(F.col("RBUNIT"), F.lit("")))
    if "RBUPTR" in pop:
        out = out.withColumn("RBUPTR", F.coalesce(F.col("RBUPTR"), F.lit("")))
    if "RCOMP" in pop:
        out = out.withColumn("RCOMP", F.coalesce(F.col("RCOMP"), F.lit("")))
    if "RITCLG" in pop:
        out = out.withColumn("RITCLG", F.coalesce(F.col("RITCLG"), F.lit("")))
    if "RITEM" in pop:
        out = out.withColumn("RITEM", F.coalesce(F.col("RITEM"), F.lit("")))
    if "FKART" in pop:
        out = out.withColumn("FKART", F.coalesce(F.col("FKART"), F.lit("F2")))
    if "VKORG" in pop:
        out = out.withColumn("VKORG", F.coalesce(F.col("VKORG"), F.lit("1000")))
    if "VTWEG" in pop:
        out = out.withColumn("VTWEG", F.coalesce(F.col("VTWEG"), F.lit("10")))
    if "SPART" in pop:
        out = out.withColumn("SPART", F.coalesce(F.col("SPART"), F.lit("00")))
    if "MATKL_MM" in pop:
        out = out.withColumn("MATKL_MM", F.coalesce(F.col("MATKL_MM"), F.lit("GEN")))
    return out


def export_supply_chain_json(flows_df: Optional[DataFrame], path: str) -> None:
    """Write supply chain flows to JSON for the Dash viewer."""
    if flows_df is None:
        return
    export_supply_chain_flows_json(flows_df, path)


def generate_acdoca_dataframe(spark: SparkSession, cfg: GenerationConfig) -> GenerationResult:
    schema = acdoca_schema()
    industry = get_industry(cfg.industry_key)
    ic_share = cfg.ic_pct if cfg.ic_pct is not None else industry.ic_share_default

    companies = build_companies(spark, cfg.country_isos, cfg.industry_key, cfg.seed)
    cidx = _companies_indexed_with_fx(spark, companies, cfg.group_currency, cfg.seed)

    n_comp = len(cfg.country_isos)
    total_lines = max(0, n_comp * 12 * cfg.txn_per_cc_per_period)
    total_lines = (total_lines // 2) * 2
    ic_lines = int(round(total_lines * ic_share / 4.0)) * 4
    if n_comp < 2:
        ic_lines = 0
    ic_lines = min(ic_lines, total_lines)
    domestic_lines = total_lines - ic_lines
    domestic_lines = (domestic_lines // 2) * 2

    n_domestic_docs = domestic_lines // 2
    n_ic_events = ic_lines // 4

    acc = spark.createDataFrame([], schema)
    sc_flows_out: Optional[DataFrame] = None

    if n_domestic_docs > 0:
        dom = domestic_balanced_documents(
            spark,
            n_domestic_docs,
            cidx,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
            cfg.include_reversals,
            industry,
        )
        if dom is not None:
            acc = acc.unionByName(_align_to_schema(dom), allowMissingColumns=True)

    if n_ic_events > 0:
        icdf = ic_paired_documents(
            spark,
            n_ic_events,
            cidx,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
        )
        if icdf is not None:
            acc = acc.unionByName(_align_to_schema(icdf), allowMissingColumns=True)

    if cfg.include_supply_chain and n_comp >= 2:
        n_chains = max(0, int(cfg.sc_chains_per_period))
        sc_flows, sc_ic = generate_supply_chain_flows(
            spark,
            n_chains,
            cidx,
            cfg.industry_key,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
        )
        if sc_ic is not None:
            acc = acc.unionByName(_align_to_schema(sc_ic), allowMissingColumns=True)
        sc_flows_out = sc_flows

    if cfg.include_closing:
        close = closing_balanced_documents(
            spark,
            cidx,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
        )
        if close is not None:
            acc = acc.unionByName(_align_to_schema(close), allowMissingColumns=True)

    periv = "V3" if cfg.fiscal_variant == "april" else "K4"
    acc = acc.withColumn("PERIV", F.coalesce(F.col("PERIV"), F.lit(periv)))

    acc = _fill_tier_defaults(acc, cfg.complexity)
    acc = _null_fields_above_tier(acc, cfg.complexity)
    return GenerationResult(acdoca_df=acc, supply_chain_flows_df=sc_flows_out)
