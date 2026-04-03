"""Financial supply chain multi-hop flows and paired IC ACDOCA rows."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.industries import canonical_industry_key
from acdoca_generator.config.materials import Material, materials_for_industry
from acdoca_generator.config.supply_chain_templates import SupplyChainStep, supply_chain_templates_for_industry
from acdoca_generator.config.tp_methods import tp_method_for_roles


def _u32(x: int) -> int:
    return x & 0xFFFFFFFF


def _pick_markup(seed: int, chain_i: int, step_i: int, buyer_i: int, lo: float, hi: float) -> float:
    h = _u32(seed ^ _u32(chain_i * 0x9E3779B1) ^ _u32(step_i * 0x85EBCA6B) ^ _u32(buyer_i * 0xC2B2AE35))
    t = (h % 1_000_000) / 1_000_000.0
    return lo + t * (hi - lo)


def _poper_i(seed: int, chain_i: int, step_i: int) -> int:
    return (_u32(seed + chain_i * 17 + step_i * 3) % 12) + 1


def _role_pools(rows: List[Any]) -> dict[str, List[Any]]:
    m: dict[str, List[Any]] = defaultdict(list)
    for r in rows:
        m[str(r.ROLE_CODE)].append(r)
    return dict(m)


def _resolve_pool(role: str, by_role: dict[str, List[Any]], all_rows: List[Any]) -> List[Any]:
    if role in by_role and by_role[role]:
        return by_role[role]
    fallbacks: dict[str, tuple[str, ...]] = {
        "TOLL": ("CMFR", "RDSC", "SSC"),
        "CMFR": ("TOLL", "FRMF"),
        "FRMF": ("IPPR", "LRD"),
        "LRD": ("FRMF", "COMM", "RHQ"),
        "IPPR": ("FRMF", "RHQ"),
        "RDSC": ("SSC", "CMFR"),
        "SSC": ("RDSC",),
        "RHQ": ("IPPR", "LRD"),
        "COMM": ("LRD", "IPPR"),
        "FINC": ("IPPR", "LRD"),
    }
    for alt in fallbacks.get(role, ()):
        if alt in by_role and by_role[alt]:
            return by_role[alt]
    return all_rows


def _first_rbukrs(pool: List[Any]) -> Optional[str]:
    if not pool:
        return None
    return str(pool[0].RBUKRS)


def sc_ic_document_schema() -> StructType:
    """Schema for synthetic supply-chain IC rows (matches intercompany shape + MATNR/WERKS/AW*)."""
    return StructType(
        [
            StructField("RBUKRS", StringType(), True),
            StructField("PRCTR", StringType(), True),
            StructField("RCNTR", StringType(), True),
            StructField("LAND1", StringType(), True),
            StructField("RHCUR", StringType(), True),
            StructField("RWCUR", StringType(), True),
            StructField("KOKRS", StringType(), True),
            StructField("SEGMENT", StringType(), True),
            StructField("RASSC", StringType(), True),
            StructField("PPRCTR", StringType(), True),
            StructField("PBUKRS", StringType(), True),
            StructField("RACCT", StringType(), True),
            StructField("WSL", DecimalType(23, 2), True),
            StructField("BSCHL", StringType(), True),
            StructField("DRCRK", StringType(), True),
            StructField("DOCLN", StringType(), True),
            StructField("BUZEI", StringType(), True),
            StructField("GJAHR", IntegerType(), True),
            StructField("RYEAR", IntegerType(), True),
            StructField("POPER", StringType(), True),
            StructField("BUDAT", DateType(), True),
            StructField("BLDAT", DateType(), True),
            StructField("FISCYEARPER", IntegerType(), True),
            StructField("PERIV", StringType(), True),
            StructField("BLART", StringType(), True),
            StructField("BSTAT", StringType(), True),
            StructField("RCLNT", StringType(), True),
            StructField("RLDNR", StringType(), True),
            StructField("RRCTY", StringType(), True),
            StructField("RMVCT", StringType(), True),
            StructField("RKCUR", StringType(), True),
            StructField("HSL", DecimalType(23, 2), True),
            StructField("KSL", DecimalType(23, 2), True),
            StructField("BELNR", StringType(), True),
            StructField("DOCNR_LD", StringType(), True),
            StructField("KOART", StringType(), True),
            StructField("GLACCOUNT_TYPE", StringType(), True),
            StructField("KTOPL", StringType(), True),
            StructField("USNAM", StringType(), True),
            StructField("TIMESTAMP", TimestampType(), True),
            StructField("SGTXT", StringType(), True),
            StructField("TAX_COUNTRY", StringType(), True),
            StructField("LIFNR", StringType(), True),
            StructField("KUNNR", StringType(), True),
            StructField("RUNIT", StringType(), True),
            StructField("MSL", DecimalType(23, 3), True),
            StructField("XREVERSING", StringType(), True),
            StructField("XREVERSED", StringType(), True),
            StructField("XTRUEREV", StringType(), True),
            StructField("AWREF", StringType(), True),
            StructField("AWTYP", StringType(), True),
            StructField("AWSYS", StringType(), True),
            StructField("AWORG", StringType(), True),
            StructField("AWITEM", StringType(), True),
            StructField("AWITGRP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )


def supply_chain_flow_schema() -> StructType:
    return StructType(
        [
            StructField("CHAIN_ID", StringType(), True),
            StructField("STEP_NUMBER", IntegerType(), True),
            StructField("MATERIAL_TYPE", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("IP_OWNER", StringType(), True),
            StructField("SELLING_COMPANY", StringType(), True),
            StructField("BUYING_COMPANY", StringType(), True),
            StructField("RESIDUAL_PROFIT_OWNER", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("TP_METHOD", StringType(), True),
            StructField("USAGE_FACTOR", DecimalType(18, 6), True),
            StructField("TOTAL_VOLUME", DecimalType(23, 3), True),
            StructField("STANDARD_COST", DecimalType(23, 4), True),
            StructField("MARKUP_RATE", DecimalType(18, 6), True),
            StructField("TOTAL_LEGAL_PRICE", DecimalType(23, 2), True),
            StructField("GJAHR", IntegerType(), True),
            StructField("POPER", StringType(), True),
            StructField("AWREF", StringType(), True),
            StructField("SELLER_ROLE", StringType(), True),
            StructField("BUYER_ROLE", StringType(), True),
            StructField("SELLER_LAND1", StringType(), True),
            StructField("BUYER_LAND1", StringType(), True),
        ]
    )


def _ic_line(
    *,
    rbukrs: str,
    prctr: str,
    rcntr: str,
    land1: str,
    rhcur: str,
    rwcur: str,
    kokrs: str,
    segment: str,
    fx_ksl: float,
    partner_bukrs: str,
    partner_prctr: str,
    racct: str,
    wsl: Decimal,
    gjahr: int,
    poper_i: int,
    belnr: str,
    group_currency: str,
    usnam: str,
    ts: datetime,
    sgtxt: str,
    matnr: str,
    werks: str,
    awref: str,
    runit: str,
    msl: Optional[Decimal],
    line_one: bool,
) -> dict[str, Any]:
    wsl_f = float(wsl)
    ksl = round(wsl_f * fx_ksl, 2)
    return {
        "RBUKRS": rbukrs,
        "PRCTR": prctr,
        "RCNTR": rcntr,
        "LAND1": land1,
        "RHCUR": rhcur,
        "RWCUR": rwcur,
        "KOKRS": kokrs,
        "SEGMENT": segment,
        "RASSC": partner_bukrs,
        "PPRCTR": partner_prctr,
        "PBUKRS": partner_bukrs,
        "RACCT": racct,
        "WSL": wsl,
        "BSCHL": "40" if line_one else "50",
        "DRCRK": "S" if line_one else "H",
        "DOCLN": "000001" if line_one else "000002",
        "BUZEI": "001" if line_one else "002",
        "GJAHR": gjahr,
        "RYEAR": gjahr,
        "POPER": str(poper_i).zfill(3),
        "BUDAT": date(gjahr, poper_i, 20),
        "BLDAT": date(gjahr, poper_i, 20),
        "FISCYEARPER": gjahr * 1000 + poper_i,
        "PERIV": "K4",
        "BLART": "AB",
        "BSTAT": " ",
        "RCLNT": "100",
        "RLDNR": "0L",
        "RRCTY": "0",
        "RMVCT": "",
        "RKCUR": group_currency,
        "HSL": wsl,
        "KSL": Decimal(str(ksl)),
        "BELNR": belnr,
        "DOCNR_LD": belnr,
        "KOART": "S",
        "GLACCOUNT_TYPE": "P",
        "KTOPL": "INTL",
        "USNAM": usnam,
        "TIMESTAMP": ts,
        "SGTXT": sgtxt,
        "TAX_COUNTRY": land1,
        "LIFNR": None,
        "KUNNR": None,
        "RUNIT": runit,
        "MSL": msl,
        "XREVERSING": "",
        "XREVERSED": "",
        "XTRUEREV": "",
        "AWREF": awref,
        "AWTYP": "BKPF",
        "AWSYS": "SYNTH_SC",
        "AWORG": "0001",
        "AWITEM": "000001",
        "AWITGRP": "",
        "MATNR": matnr,
        "WERKS": werks,
    }


def _four_ic_rows_for_hop(
    buyer: Any,
    seller: Any,
    *,
    amt: Decimal,
    gjahr: int,
    poper_i: int,
    seed: int,
    event_key: int,
    group_currency: str,
    mat: Material,
    werks: str,
    awref: str,
    chain_id: str,
    step_number: int,
) -> List[dict[str, Any]]:
    """Buyer = company A (inventory), Seller = company B (revenue). amt > 0."""
    b_bukrs = str(buyer.RBUKRS)
    s_bukrs = str(seller.RBUKRS)
    b_fx = float(buyer.FX_KSL)
    s_fx = float(seller.FX_KSL)
    buyer_belnr = str(6_000_000_000 + (event_key % 3_999_000_000)).zfill(10)
    seller_belnr = str(int(buyer_belnr) + 1).zfill(10)
    ts = datetime.fromtimestamp((event_key % 100_000_000) + seed * 1000)
    sgtxt = f"SC {chain_id} step {step_number} {mat.matnr}"
    runit = mat.unit
    msl_dec = amt / Decimal(str(mat.standard_cost)) if mat.standard_cost > 0 else None

    rows: List[dict[str, Any]] = []
    rows.append(
        _ic_line(
            rbukrs=b_bukrs,
            prctr=str(buyer.PRCTR),
            rcntr=str(buyer.RCNTR),
            land1=str(buyer.LAND1),
            rhcur=str(buyer.RHCUR),
            rwcur=str(buyer.RWCUR),
            kokrs=str(buyer.KOKRS),
            segment=str(buyer.SEGMENT),
            fx_ksl=b_fx,
            partner_bukrs=s_bukrs,
            partner_prctr=str(seller.PRCTR),
            racct=SAMPLE_GL["inventory_fg"],
            wsl=amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=buyer_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=mat.matnr,
            werks=werks,
            awref=awref,
            runit=runit,
            msl=msl_dec,
            line_one=True,
        )
    )
    rows.append(
        _ic_line(
            rbukrs=b_bukrs,
            prctr=str(buyer.PRCTR),
            rcntr=str(buyer.RCNTR),
            land1=str(buyer.LAND1),
            rhcur=str(buyer.RHCUR),
            rwcur=str(buyer.RWCUR),
            kokrs=str(buyer.KOKRS),
            segment=str(buyer.SEGMENT),
            fx_ksl=b_fx,
            partner_bukrs=s_bukrs,
            partner_prctr=str(seller.PRCTR),
            racct=SAMPLE_GL["ap_ic"],
            wsl=-amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=buyer_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=mat.matnr,
            werks="",
            awref=awref,
            runit=runit,
            msl=None,
            line_one=False,
        )
    )
    rows.append(
        _ic_line(
            rbukrs=s_bukrs,
            prctr=str(seller.PRCTR),
            rcntr=str(seller.RCNTR),
            land1=str(seller.LAND1),
            rhcur=str(seller.RHCUR),
            rwcur=str(seller.RWCUR),
            kokrs=str(seller.KOKRS),
            segment=str(seller.SEGMENT),
            fx_ksl=s_fx,
            partner_bukrs=b_bukrs,
            partner_prctr=str(buyer.PRCTR),
            racct=SAMPLE_GL["ar_ic"],
            wsl=amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=seller_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=mat.matnr,
            werks=werks,
            awref=awref,
            runit=runit,
            msl=None,
            line_one=True,
        )
    )
    rows.append(
        _ic_line(
            rbukrs=s_bukrs,
            prctr=str(seller.PRCTR),
            rcntr=str(seller.RCNTR),
            land1=str(seller.LAND1),
            rhcur=str(seller.RHCUR),
            rwcur=str(seller.RWCUR),
            kokrs=str(seller.KOKRS),
            segment=str(seller.SEGMENT),
            fx_ksl=s_fx,
            partner_bukrs=b_bukrs,
            partner_prctr=str(buyer.PRCTR),
            racct=SAMPLE_GL["revenue_ic"],
            wsl=-amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=seller_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=mat.matnr,
            werks="",
            awref=awref,
            runit=runit,
            msl=None,
            line_one=False,
        )
    )
    return rows


def generate_supply_chain_flows(
    spark: SparkSession,
    n_chains: int,
    companies_indexed: DataFrame,
    industry_key: str,
    gjahr: int,
    seed: int,
    group_currency: str,
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """
    Build supply chain hop rows and paired IC ACDOCA-style rows.
    Returns (flows_df, ic_df) or (None, None) if not enough companies or n_chains <= 0.
    """
    if n_chains <= 0:
        return None, None

    rows = companies_indexed.collect()
    if len(rows) < 2:
        return None, None

    ck = canonical_industry_key(industry_key)
    templates = supply_chain_templates_for_industry(ck)
    materials = materials_for_industry(ck)
    by_role = _role_pools(rows)

    ip_pool = by_role.get("IPPR") or by_role.get("RHQ") or rows[:1]
    ip_owner = _first_rbukrs(ip_pool) or str(rows[0].RBUKRS)
    residual = ip_owner

    flow_tuples: List[tuple] = []
    ic_rows: List[dict[str, Any]] = []
    event_counter = 0

    for chain_i in range(n_chains):
        tpl: Tuple[SupplyChainStep, ...] = templates[chain_i % len(templates)]
        chain_id = f"SC{chain_i + 1:06d}"
        base_v = 800.0 + float(_u32(seed + chain_i * 31) % 4200)
        v_carry = base_v

        for step_i, step in enumerate(tpl):
            mat = materials[step.material_index] if step.material_index < len(materials) else materials[0]
            sellers = _resolve_pool(step.source_role, by_role, rows) or rows
            buyers_pool = _resolve_pool(step.dest_role, by_role, rows) or rows

            si = _u32(seed + chain_i * 19 + step_i * 7) % len(sellers)
            seller = sellers[si]

            if step.dest_role == "LRD" and len(buyers_pool) > 1:
                buyer_candidates = [b for b in buyers_pool if str(b.RBUKRS) != str(seller.RBUKRS)]
                if not buyer_candidates:
                    buyer_candidates = list(buyers_pool)
            else:
                bi = _u32(seed + chain_i * 13 + step_i * 5) % len(buyers_pool)
                buyer = buyers_pool[bi]
                if str(buyer.RBUKRS) == str(seller.RBUKRS):
                    buyer = buyers_pool[(bi + 1) % len(buyers_pool)]
                buyer_candidates = [buyer]

            tp = tp_method_for_roles(step.source_role, step.dest_role)
            poper_i = _poper_i(seed, chain_i, step_i)
            poper_s = str(poper_i).zfill(3)

            vol_this = float(v_carry)
            if step.usage_factor and step.usage_factor != 0:
                vol_this = float(v_carry) / float(step.usage_factor)

            buyer_candidates = [b for b in buyer_candidates if str(b.RBUKRS) != str(seller.RBUKRS)]
            if not buyer_candidates:
                buyer_candidates = [b for b in rows if str(b.RBUKRS) != str(seller.RBUKRS)][:1]
            if not buyer_candidates:
                break

            n_buyers = len(buyer_candidates)
            for bi, buyer in enumerate(buyer_candidates):
                vol = vol_this / n_buyers if n_buyers else vol_this
                markup = _pick_markup(seed, chain_i, step_i, bi, tp.markup_low, tp.markup_high)
                std = Decimal(str(mat.standard_cost))
                mup = Decimal(str(markup))
                vol_dec = Decimal(str(round(vol, 6)))
                legal = (std * (Decimal("1") + mup) * vol_dec).quantize(Decimal("0.01"))

                awref = f"SC{chain_i * 10_000 + step_i * 100 + bi:09d}"
                werks = f"P{str(seller.RBUKRS)}"

                flow_tuples.append(
                    (
                        chain_id,
                        step.step_number,
                        step.material_type,
                        mat.matnr,
                        ip_owner,
                        str(seller.RBUKRS),
                        str(buyer.RBUKRS),
                        residual,
                        werks,
                        tp.code,
                        Decimal(str(step.usage_factor)),
                        vol_dec,
                        std,
                        mup,
                        legal,
                        gjahr,
                        poper_s,
                        awref,
                        step.source_role,
                        step.dest_role,
                        str(seller.LAND1),
                        str(buyer.LAND1),
                    )
                )

                ic_rows.extend(
                    _four_ic_rows_for_hop(
                        buyer,
                        seller,
                        amt=legal,
                        gjahr=gjahr,
                        poper_i=poper_i,
                        seed=seed,
                        event_key=event_counter,
                        group_currency=group_currency,
                        mat=mat,
                        werks=werks,
                        awref=awref,
                        chain_id=chain_id,
                        step_number=step.step_number,
                    )
                )
                event_counter += 1

            v_carry = float(vol_this)

    if not flow_tuples:
        return None, None

    flows_df = spark.createDataFrame(flow_tuples, schema=supply_chain_flow_schema())
    ic_df = spark.createDataFrame(ic_rows, schema=sc_ic_document_schema())
    return flows_df, ic_df


def export_supply_chain_flows_json(flows_df: DataFrame, path: str) -> None:
    """Write supply chain flows to JSON (records array) for the Dash app."""
    pdf = flows_df.toPandas()
    pdf.to_json(path, orient="records", indent=2, date_format="iso")
