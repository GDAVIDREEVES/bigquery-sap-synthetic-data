"""Financial supply chain multi-hop flows and paired IC ACDOCA rows."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
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
from acdoca_generator.config.tp_methods import TPMethod, tp_method_for_roles


@dataclass(frozen=True)
class LegSpec:
    """GL routing for the four lines of an IC document, per transaction type.

    Naming follows the doc shape: lines 1-2 sit at the buyer (DR expense/asset,
    CR ap_ic), lines 3-4 sit at the seller (DR ar_ic, CR revenue/income).
    """
    buyer_dr_acct: str
    buyer_cr_acct: str
    seller_dr_acct: str
    seller_cr_acct: str
    seller_cr_rfarea: str          # RFAREA on the seller's revenue line (drives segment_pl bucket)
    populate_goods_fields: bool    # True → MATNR/WERKS/RUNIT/MSL populated; False → blanked


_LEGSPEC_GOODS = LegSpec(
    buyer_dr_acct=SAMPLE_GL["inventory_fg"],
    buyer_cr_acct=SAMPLE_GL["ap_ic"],
    seller_dr_acct=SAMPLE_GL["ar_ic"],
    seller_cr_acct=SAMPLE_GL["revenue_ic"],
    seller_cr_rfarea="0200",
    populate_goods_fields=True,
)

_LEGSPEC_ROYALTY = LegSpec(
    buyer_dr_acct=SAMPLE_GL["royalty_exp"],
    buyer_cr_acct=SAMPLE_GL["ap_ic"],
    seller_dr_acct=SAMPLE_GL["ar_ic"],
    seller_cr_acct=SAMPLE_GL["revenue_royalty"],
    seller_cr_rfarea="0500",  # Marketing — royalty income classified under brand/IP commercialization
    populate_goods_fields=False,
)

_LEGSPEC_SERVICE = LegSpec(
    buyer_dr_acct=SAMPLE_GL["ic_service_exp"],
    buyer_cr_acct=SAMPLE_GL["ap_ic"],
    seller_dr_acct=SAMPLE_GL["ar_ic"],
    seller_cr_acct=SAMPLE_GL["revenue_service"],
    seller_cr_rfarea="0300",  # Administration — IC service / mgmt fee
    populate_goods_fields=False,
)

_LEGSPEC_COST_SHARE = LegSpec(
    buyer_dr_acct=SAMPLE_GL["opex_rd"],
    buyer_cr_acct=SAMPLE_GL["ap_ic"],
    seller_dr_acct=SAMPLE_GL["ar_ic"],
    seller_cr_acct=SAMPLE_GL["revenue_ic"],
    seller_cr_rfarea="0400",  # R&D — cost-share recharge offset
    populate_goods_fields=False,
)

_LEGSPECS: dict[str, LegSpec] = {
    "goods": _LEGSPEC_GOODS,
    "royalty": _LEGSPEC_ROYALTY,
    "service": _LEGSPEC_SERVICE,
    "cost_share": _LEGSPEC_COST_SHARE,
}

_AWREF_PREFIX: dict[str, str] = {
    "goods": "SC",
    "royalty": "RY",
    "service": "SV",
    "cost_share": "CS",
}


def legspec_for(transaction_type: str) -> LegSpec:
    """Return the LegSpec for a transaction_type, defaulting to goods on unknown."""
    return _LEGSPECS.get(transaction_type, _LEGSPEC_GOODS)


def _u32(x: int) -> int:
    return x & 0xFFFFFFFF


def _pick_markup(seed: int, chain_i: int, step_i: int, buyer_i: int, lo: float, hi: float) -> float:
    """Symmetric triangular distribution in [lo, hi] (mode at midpoint).

    Uses sum-of-two-uniforms — better matches benchmarking-study median behavior than
    a flat uniform draw, which overstates how clean real-world margin data is.
    """
    h1 = _u32(seed ^ _u32(chain_i * 0x9E3779B1) ^ _u32(step_i * 0x85EBCA6B) ^ _u32(buyer_i * 0xC2B2AE35))
    h2 = _u32(h1 * 0xDEADBEEF + chain_i * 0x12345)
    u1 = (h1 % 1_000_000) / 1_000_000.0
    u2 = (h2 % 1_000_000) / 1_000_000.0
    t = (u1 + u2) / 2.0
    return lo + t * (hi - lo)


def _chain_start_poper(seed: int, chain_i: int) -> int:
    """Each chain anchors to one starting period; step events follow sequentially."""
    return (_u32(seed + chain_i * 17) % 12) + 1


def _poper_for_step(seed: int, chain_i: int, step_i: int) -> int:
    """Step's POPER = min(12, chain_start + step_i). Causal ordering within a chain.

    A chain starting in POPER 11 with 3 steps lands events on 11, 12, 12 (clamp at year-end).
    """
    return min(12, _chain_start_poper(seed, chain_i) + step_i)


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
            StructField("RFAREA", StringType(), True),
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
            StructField("APA_FLAG", BooleanType(), True),
            StructField("CHALLENGED_FLAG", BooleanType(), True),
            StructField("ADJUSTED_VIEW_PRICE", DecimalType(23, 2), True),
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
    rfarea: str,
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
        "RFAREA": rfarea,
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
    mat: Optional[Material],
    werks: str,
    awref: str,
    chain_id: str,
    step_number: int,
    legspec: LegSpec = _LEGSPEC_GOODS,
    transaction_type: str = "goods",
) -> List[dict[str, Any]]:
    """Emit the 4 paired IC lines for one buyer/seller hop.

    Buyer takes line 1 (DR — inventory for goods, expense for non-goods)
    and line 2 (CR ap_ic). Seller takes line 3 (DR ar_ic) and line 4
    (CR — revenue/income). All four share the same WSL magnitude so the
    document nets to 0.

    For non-goods flows (`legspec.populate_goods_fields=False`), MATNR /
    WERKS / RUNIT / MSL are blanked on every line — those fields only have
    SAP-side meaning for material movements.
    """
    b_bukrs = str(buyer.RBUKRS)
    s_bukrs = str(seller.RBUKRS)
    b_fx = float(buyer.FX_KSL)
    s_fx = float(seller.FX_KSL)
    buyer_belnr = str(6_000_000_000 + (event_key % 3_999_000_000)).zfill(10)
    seller_belnr = str(int(buyer_belnr) + 1).zfill(10)
    ts = datetime.fromtimestamp((event_key % 100_000_000) + seed * 1000)

    is_goods = legspec.populate_goods_fields
    matnr_val = mat.matnr if (is_goods and mat is not None) else ""
    runit_val = mat.unit if (is_goods and mat is not None) else ""
    werks_seller_leg = werks if is_goods else ""
    msl_dec: Optional[Decimal] = None
    if is_goods and mat is not None and mat.standard_cost > 0:
        msl_dec = amt / Decimal(str(mat.standard_cost))
    label = matnr_val if is_goods else transaction_type.upper()
    sgtxt = f"{transaction_type.upper()} {chain_id} step {step_number} {label}".strip()

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
            racct=legspec.buyer_dr_acct,
            rfarea="",
            wsl=amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=buyer_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=matnr_val,
            werks=werks_seller_leg,
            awref=awref,
            runit=runit_val,
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
            racct=legspec.buyer_cr_acct,
            rfarea="",
            wsl=-amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=buyer_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=matnr_val,
            werks="",
            awref=awref,
            runit=runit_val,
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
            racct=legspec.seller_dr_acct,
            rfarea="",
            wsl=amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=seller_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=matnr_val,
            werks=werks_seller_leg,
            awref=awref,
            runit=runit_val,
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
            racct=legspec.seller_cr_acct,
            rfarea=legspec.seller_cr_rfarea,
            wsl=-amt,
            gjahr=gjahr,
            poper_i=poper_i,
            belnr=seller_belnr,
            group_currency=group_currency,
            usnam="SYNTH_SC",
            ts=ts,
            sgtxt=sgtxt,
            matnr=matnr_val,
            werks="",
            awref=awref,
            runit=runit_val,
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
    challenged_share: float = 0.0,
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """
    Build supply chain hop rows and paired IC ACDOCA-style rows.

    `challenged_share` (0.0–1.0) tags that fraction of flows as "challenged"
    by a tax authority — populating CHALLENGED_FLAG and ADJUSTED_VIEW_PRICE on
    the flow row (not on the IC ACDOCA rows; the controversy is a metadata
    overlay, not a posted journal entry). Tagging is deterministic by seed.

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

            should_fanout = step.fanout_all or (step.dest_role == "LRD" and len(buyers_pool) > 1)
            if should_fanout:
                buyer_candidates = [b for b in buyers_pool if str(b.RBUKRS) != str(seller.RBUKRS)]
                if not buyer_candidates:
                    buyer_candidates = list(buyers_pool)
            else:
                bi = _u32(seed + chain_i * 13 + step_i * 5) % len(buyers_pool)
                buyer = buyers_pool[bi]
                if str(buyer.RBUKRS) == str(seller.RBUKRS):
                    buyer = buyers_pool[(bi + 1) % len(buyers_pool)]
                buyer_candidates = [buyer]

            tp = tp_method_for_roles(step.source_role, step.dest_role, step.transaction_type)
            # APA-covered steps use the tighter declared band (typically lower / pre-agreed)
            # instead of the role-pair-derived band. Other downstream behavior is unchanged.
            if step.apa_flag and step.apa_markup_band is not None:
                apa_lo, apa_hi = step.apa_markup_band
                tp = TPMethod(
                    code=tp.code + "/APA", description=tp.description + " (APA)",
                    markup_low=apa_lo, markup_high=apa_hi,
                )
            poper_i = _poper_for_step(seed, chain_i, step_i)
            poper_s = str(poper_i).zfill(3)

            vol_this = float(v_carry)
            if step.usage_factor and step.usage_factor != 0:
                vol_this = float(v_carry) / float(step.usage_factor)

            buyer_candidates = [b for b in buyer_candidates if str(b.RBUKRS) != str(seller.RBUKRS)]
            if not buyer_candidates:
                buyer_candidates = [b for b in rows if str(b.RBUKRS) != str(seller.RBUKRS)][:1]
            if not buyer_candidates:
                break

            legspec = legspec_for(step.transaction_type)
            awref_prefix = _AWREF_PREFIX.get(step.transaction_type, "SC")

            n_buyers = len(buyer_candidates)
            for bi, buyer in enumerate(buyer_candidates):
                vol = vol_this / n_buyers if n_buyers else vol_this
                markup = _pick_markup(seed, chain_i, step_i, bi, tp.markup_low, tp.markup_high)
                std = Decimal(str(mat.standard_cost))
                mup = Decimal(str(markup))
                vol_dec = Decimal(str(round(vol, 6)))

                # Legal amount formula varies by transaction_type:
                #   goods    : cost × (1 + markup) × volume
                #   service  : same shape (cost-plus on a service cost base)
                #   royalty  : base × rate (no `1 +`); rate sourced from step or markup band
                #   cost_share : base × pool_share (recharge at-cost; markup band ≈ 0)
                if step.transaction_type == "royalty":
                    rate = Decimal(str(step.royalty_rate)) if step.royalty_rate is not None else mup
                    legal = (std * vol_dec * rate).quantize(Decimal("0.01"))
                elif step.transaction_type == "cost_share":
                    share = Decimal(str(step.cost_pool_share)) if step.cost_pool_share is not None else Decimal("1.0")
                    legal = (std * vol_dec * share).quantize(Decimal("0.01"))
                else:  # goods, service, or unknown → cost-plus shape
                    legal = (std * (Decimal("1") + mup) * vol_dec).quantize(Decimal("0.01"))

                awref = f"{awref_prefix}{chain_i * 10_000 + step_i * 100 + bi:09d}"
                werks = f"P{str(seller.RBUKRS)}" if legspec.populate_goods_fields else ""
                matnr_for_flow = mat.matnr if legspec.populate_goods_fields else ""

                # Controversy tagging — deterministic by seed/chain/step/buyer.
                # CHALLENGED flows get a 15-30% adjusted-view uplift representing a
                # tax authority's recharacterization. APA flows are never challenged.
                challenged = False
                adjusted_view: Optional[Decimal] = None
                if challenged_share > 0.0 and not step.apa_flag:
                    h = _u32(seed ^ _u32(chain_i * 0xA5A5) ^ _u32(step_i * 0x5A5A) ^ _u32(bi * 0xC3C3))
                    if (h % 1_000_000) / 1_000_000.0 < challenged_share:
                        challenged = True
                        h2 = _u32(h * 0xDEADBEEF + chain_i * 7919)
                        uplift_pct = 0.15 + ((h2 % 1_000_000) / 1_000_000.0) * 0.15  # 15–30%
                        adjusted_view = (legal * (Decimal("1") + Decimal(str(uplift_pct)))).quantize(Decimal("0.01"))

                flow_tuples.append(
                    (
                        chain_id,
                        step.step_number,
                        step.material_type,
                        matnr_for_flow,
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
                        bool(step.apa_flag),
                        bool(challenged),
                        adjusted_view,
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
                        legspec=legspec,
                        transaction_type=step.transaction_type,
                    )
                )
                event_counter += 1

            v_carry = float(vol_this)

    if not flow_tuples:
        return None, None

    flows_df = spark.createDataFrame(flow_tuples, schema=supply_chain_flow_schema())
    ic_df = spark.createDataFrame(ic_rows, schema=sc_ic_document_schema())
    return flows_df, ic_df


def _flow_json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"unserializable type {type(obj).__name__}")


def export_supply_chain_flows_json(flows_df: DataFrame, path: str) -> None:
    """Write supply chain flows to JSON (records array) for the Dash viewer.

    Avoids ``DataFrame.toPandas()`` so the helper works on Python 3.12, where
    PySpark 3.5's pandas-conversion path imports the removed-stdlib ``distutils``.
    """
    records = [row.asDict(recursive=True) for row in flows_df.collect()]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=_flow_json_default, ensure_ascii=False)
