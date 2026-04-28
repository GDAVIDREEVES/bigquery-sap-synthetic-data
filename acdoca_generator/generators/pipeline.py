"""End-to-end ACDOCA generation (SPEC §2, §10)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
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
    include_supply_chain: bool = True
    sc_chains_per_period: int = 50
    include_segment_pl: bool = False
    include_year_end_trueup: bool = True
    challenged_share: float = 0.0  # 0.0–1.0; fraction of SC flows tagged as "challenged" by tax authority


@dataclass
class GenerationResult:
    acdoca_df: DataFrame
    supply_chain_flows_df: Optional[DataFrame] = None
    segment_pl_df: Optional[DataFrame] = None
    entity_roles_df: Optional[DataFrame] = None


def _companies_indexed_with_fx(
    spark: SparkSession, companies_df: DataFrame, group_currency: str, seed: int
) -> DataFrame:
    # Avoid Window.orderBy-only (single-partition shuffle); companies list is tiny.
    rows = companies_df.orderBy("RBUKRS").collect()
    ci_df = spark.createDataFrame(
        [(r.RBUKRS, i) for i, r in enumerate(rows)],
        ["RBUKRS", "ci"],
    )
    fx_df = spark.createDataFrame(
        [(r.RBUKRS, float(fx_multiplier(r.RHCUR, group_currency, seed, 1))) for r in rows],
        ["RBUKRS", "FX_KSL"],
    )
    return companies_df.join(ci_df, "RBUKRS", "inner").join(fx_df, "RBUKRS", "left")


def _tier_default_columns() -> dict[str, "F.Column"]:
    """Tier-fill defaults: SQL field name → Column expression for `F.coalesce(col, default)`.

    Lazy-built per call (Column refs are cheap; building once in a helper avoids
    a module-level reference to F.col which would race with pyspark imports).
    """
    return {
        "VORGN": F.lit("RFBU"),
        "VRGNG": F.lit("RFBU"),
        "BTTYPE": F.lit(""),
        "LINETYPE": F.lit(""),
        "KTOSL": F.lit(""),
        "MWSKZ": F.lit("V0"),
        "NETDT": F.col("BUDAT"),
        "VALUT": F.col("BUDAT"),
        "SDM_VERSION": F.lit("1"),
        "RFCCUR": F.col("RHCUR"),
        "FCSL": F.col("HSL").cast(DecimalType(23, 2)),
        "RBUNIT": F.lit(""),
        "RBUPTR": F.lit(""),
        "RCOMP": F.lit(""),
        "RITCLG": F.lit(""),
        "RITEM": F.lit(""),
        "FKART": F.lit("F2"),
        "VKORG": F.lit("1000"),
        "VTWEG": F.lit("10"),
        "SPART": F.lit("00"),
        "MATKL_MM": F.lit("GEN"),
    }


def _apply_tier(df: DataFrame, complexity: str) -> DataFrame:
    """Tier-fill defaults + null-above-tier in one ``select`` (single catalyst node).

    Replaces the prior ``_fill_tier_defaults`` + ``_null_fields_above_tier`` chain,
    which together produced 250–600 ``withColumn`` ops per call (one per schema
    field) and exploded the catalyst plan on larger presets.
    """
    populated = fields_for_complexity(complexity)
    excl = excluded_sql_names()
    schema = acdoca_schema()
    defaults = _tier_default_columns()
    select_exprs = []
    for field in schema.fields:
        name = field.name
        if name in excl or name not in populated:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(name))
        elif name in defaults:
            select_exprs.append(F.coalesce(F.col(name), defaults[name]).alias(name))
        else:
            select_exprs.append(F.col(name))
    return df.select(*select_exprs)


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
            n_comp=n_comp,
        )
        if dom is not None:
            acc = acc.unionByName(dom)

    if n_ic_events > 0:
        icdf = ic_paired_documents(
            spark,
            n_ic_events,
            cidx,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
            n_comp=n_comp,
        )
        if icdf is not None:
            acc = acc.unionByName(icdf)

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
            challenged_share=float(cfg.challenged_share),
        )
        if sc_ic is not None:
            acc = acc.unionByName(sc_ic)
        sc_flows_out = sc_flows

    if cfg.include_closing:
        close = closing_balanced_documents(
            spark,
            cidx,
            cfg.fiscal_year,
            cfg.seed,
            cfg.group_currency,
            n_comp=n_comp,
        )
        if close is not None:
            acc = acc.unionByName(close)

    periv = "V3" if cfg.fiscal_variant == "april" else "K4"
    acc = acc.withColumn("PERIV", F.coalesce(F.col("PERIV"), F.lit(periv)))

    acc = _apply_tier(acc, cfg.complexity)

    if cfg.include_year_end_trueup and n_comp >= 2:
        from acdoca_generator.generators.year_end_trueup import year_end_trueup_documents
        tu_df = year_end_trueup_documents(
            spark, acc, cidx, cfg.fiscal_year, cfg.seed, cfg.group_currency,
        )
        if tu_df is not None:
            acc = acc.unionByName(tu_df)

    seg_pl_out: Optional[DataFrame] = None
    if cfg.include_segment_pl:
        from acdoca_generator.aggregations.segment_pl import build_segment_pl
        seg_pl_out = build_segment_pl(acc, companies)

    from acdoca_generator.aggregations.entity_roles import build_entity_roles
    entity_roles_out = build_entity_roles(companies)

    return GenerationResult(
        acdoca_df=acc,
        supply_chain_flows_df=sc_flows_out,
        segment_pl_df=seg_pl_out,
        entity_roles_df=entity_roles_out,
    )
