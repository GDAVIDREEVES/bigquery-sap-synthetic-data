"""Year-end transfer-pricing true-up adjustments.

Real principal structures squeeze or credit their LRDs at year-end so each
distributor lands inside its target operating-margin band (e.g. LRD 2-4%).
The mechanism is a Q4 manual journal: an IC adjustment between the LRD and
the principal (IPPR), tagged so a TP product can identify it as a true-up
rather than a routine month-end accrual.

This module reads the post-tier-fill ACDOCA accumulator, computes each
LRD entity's full-year operating margin via segment_pl, and emits a 4-line
paired adjustment when the margin is outside the band. The principal is
the offsetting counterparty (any IPPR-coded entity in the run; if none
exists, no true-ups are generated).

JE shape (BLART="SA" manual journal, AWREF prefix "TU"):
    LRD margin too LOW  → IPPR pays LRD an income credit
        line 1 (LRD):    DR ar_ic                 +adj
        line 2 (LRD):    CR revenue_ic            -adj
        line 3 (IPPR):   DR mgmt_fee_exp          +adj
        line 4 (IPPR):   CR ap_ic                 -adj
    LRD margin too HIGH → LRD pays IPPR a margin clawback
        line 1 (LRD):    DR mgmt_fee_exp          +adj
        line 2 (LRD):    CR ap_ic                 -adj
        line 3 (IPPR):   DR ar_ic                 +adj
        line 4 (IPPR):   CR revenue_ic            -adj

POPER for emitted rows is fixed at 12 (Q4 close). Adjustment magnitude
brings the LRD's margin to the **midpoint** of its target band.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from acdoca_generator.aggregations.segment_pl import build_segment_pl
from acdoca_generator.config.chart_of_accounts import SAMPLE_GL
from acdoca_generator.config.operating_models import ROLE_BY_CODE
from acdoca_generator.generators.supply_chain import sc_ic_document_schema
from acdoca_generator.utils.schema import align_to_acdoca


_TRUEUP_AWREF_PREFIX = "TU"
_TRUEUP_POPER = 12  # Q4 close


def _u32(x: int) -> int:
    return x & 0xFFFFFFFF


def _trueup_je_rows(
    *,
    lrd_row: Any,
    principal_row: Any,
    adjustment: Decimal,
    gjahr: int,
    seed: int,
    event_key: int,
    group_currency: str,
    awref: str,
) -> List[dict[str, Any]]:
    """Build the 4-line paired JE for one LRD true-up.

    `adjustment > 0` → IPPR pays LRD (LRD margin was too low; income credit).
    `adjustment < 0` → LRD pays IPPR (LRD margin was too high; clawback).

    The signed amount drives the GL routing; line WSL values still net to 0
    per BELNR.
    """
    if adjustment == 0:
        return []
    abs_amt = abs(adjustment)

    # Direction-aware account routing
    if adjustment > 0:
        lrd_dr_acct = SAMPLE_GL["ar_ic"]
        lrd_cr_acct = SAMPLE_GL["revenue_ic"]
        lrd_cr_rfarea = "0200"
        ip_dr_acct = SAMPLE_GL["mgmt_fee_exp"]
        ip_cr_acct = SAMPLE_GL["ap_ic"]
        ip_dr_rfarea = "0300"
    else:
        lrd_dr_acct = SAMPLE_GL["mgmt_fee_exp"]
        lrd_cr_acct = SAMPLE_GL["ap_ic"]
        lrd_cr_rfarea = ""
        ip_dr_acct = SAMPLE_GL["ar_ic"]
        ip_cr_acct = SAMPLE_GL["revenue_ic"]
        ip_dr_rfarea = "0200"

    lrd_belnr = str(7_000_000_000 + (event_key % 999_000_000)).zfill(10)
    ip_belnr = str(int(lrd_belnr) + 1).zfill(10)
    ts = datetime.fromtimestamp((event_key % 100_000_000) + seed * 1000)
    sgtxt = f"TRUEUP {gjahr} LRD={lrd_row.RBUKRS}"

    rows: List[dict[str, Any]] = []
    # LRD lines (1, 2)
    rows.append(_make_row(lrd_row, principal_row, lrd_dr_acct, abs_amt, lrd_belnr,
                          gjahr, group_currency, awref, sgtxt, ts, "0", line_one=True))
    rows.append(_make_row(lrd_row, principal_row, lrd_cr_acct, -abs_amt, lrd_belnr,
                          gjahr, group_currency, awref, sgtxt, ts, lrd_cr_rfarea, line_one=False))
    # IPPR lines (3, 4)
    rows.append(_make_row(principal_row, lrd_row, ip_dr_acct, abs_amt, ip_belnr,
                          gjahr, group_currency, awref, sgtxt, ts, ip_dr_rfarea, line_one=True))
    rows.append(_make_row(principal_row, lrd_row, ip_cr_acct, -abs_amt, ip_belnr,
                          gjahr, group_currency, awref, sgtxt, ts, "0", line_one=False))
    return rows


def _make_row(
    self_row: Any,
    partner_row: Any,
    racct: str,
    wsl: Decimal,
    belnr: str,
    gjahr: int,
    group_currency: str,
    awref: str,
    sgtxt: str,
    ts: datetime,
    rfarea: str,
    *,
    line_one: bool,
) -> dict[str, Any]:
    fx = float(getattr(self_row, "FX_KSL", 1.0))
    ksl = round(float(wsl) * fx, 2)
    return {
        "RBUKRS": str(self_row.RBUKRS),
        "PRCTR": str(self_row.PRCTR),
        "RCNTR": str(self_row.RCNTR),
        "LAND1": str(self_row.LAND1),
        "RHCUR": str(self_row.RHCUR),
        "RWCUR": str(self_row.RWCUR),
        "KOKRS": str(self_row.KOKRS),
        "SEGMENT": str(self_row.SEGMENT),
        "RASSC": str(partner_row.RBUKRS),
        "PPRCTR": str(partner_row.PRCTR),
        "PBUKRS": str(partner_row.RBUKRS),
        "RACCT": racct,
        "RFAREA": rfarea if rfarea else "",
        "WSL": Decimal(str(wsl)),
        "BSCHL": "40" if line_one else "50",
        "DRCRK": "S" if line_one else "H",
        "DOCLN": "000001" if line_one else "000002",
        "BUZEI": "001" if line_one else "002",
        "GJAHR": gjahr,
        "RYEAR": gjahr,
        "POPER": str(_TRUEUP_POPER).zfill(3),
        "BUDAT": date(gjahr, _TRUEUP_POPER, 28),
        "BLDAT": date(gjahr, _TRUEUP_POPER, 28),
        "FISCYEARPER": gjahr * 1000 + _TRUEUP_POPER,
        "PERIV": "K4",
        "BLART": "SA",  # manual journal — distinguishes trueup from routine "AB"
        "BSTAT": " ",
        "RCLNT": "100",
        "RLDNR": "0L",
        "RRCTY": "0",
        "RMVCT": "",
        "RKCUR": group_currency,
        "HSL": Decimal(str(wsl)),
        "KSL": Decimal(str(ksl)),
        "BELNR": belnr,
        "DOCNR_LD": belnr,
        "KOART": "S",
        "GLACCOUNT_TYPE": "P",
        "KTOPL": "INTL",
        "USNAM": "SYNTH_TU",
        "TIMESTAMP": ts,
        "SGTXT": sgtxt,
        "TAX_COUNTRY": str(self_row.LAND1),
        "LIFNR": None,
        "KUNNR": None,
        "RUNIT": "",
        "MSL": None,
        "XREVERSING": "",
        "XREVERSED": "",
        "XTRUEREV": "",
        "AWREF": awref,
        "AWTYP": "BKPF",
        "AWSYS": "SYNTH_TU",
        "AWORG": "0001",
        "AWITEM": "000001",
        "AWITGRP": "",
        "MATNR": "",
        "WERKS": "",
    }


def trueup_adjustment(current_margin: float, om_low: float, om_high: float) -> float:
    """Return the *additive margin gap* needed to bring `current_margin` to the band midpoint.

    Returns 0.0 if `current_margin` is already inside [om_low, om_high].
    Positive return → margin should go up (LRD is undercompensated).
    Negative return → margin should go down (LRD is overcompensated).
    """
    if om_low <= current_margin <= om_high:
        return 0.0
    target = (om_low + om_high) / 2.0
    return target - current_margin


def year_end_trueup_documents(
    spark: SparkSession,
    acdoca_df: DataFrame,
    companies_df: DataFrame,
    gjahr: int,
    seed: int,
    group_currency: str,
) -> Optional[DataFrame]:
    """Generate Q4 paired JEs to bring LRD margins into target band.

    Returns None when no principal entity exists in the run, when no LRDs
    are present, or when every LRD's margin is already inside the band.
    """
    role_targets = {code: r for code, r in ROLE_BY_CODE.items()}
    lrd_target = role_targets.get("LRD")
    if lrd_target is None:
        return None

    company_rows = companies_df.collect()
    principal = next((r for r in company_rows if str(r.ROLE_CODE) == "IPPR"), None)
    if principal is None:
        return None  # nothing to true up against

    # Persist the upstream accumulator before triggering segment_pl's collect.
    # Without this, the entire 538-col plan is re-evaluated for each downstream
    # action; persisting breaks the lineage so Catalyst can optimize each chunk
    # independently. We unpersist in finally so notebook reruns don't leak.
    acdoca_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # Per-entity full-year op profit and revenue total (sum across POPERs)
        seg_pl = build_segment_pl(acdoca_df, companies_df)
        yearly = (
            seg_pl
            .filter(F.col("ROLE_CODE") == "LRD")
            .groupBy("RBUKRS", "ROLE_CODE")
            .agg(
                F.sum(F.col("revenue") + F.col("other_income")).alias("rev_y"),
                F.sum("operating_profit").alias("op_y"),
            )
            .collect()
        )
    finally:
        acdoca_df.unpersist()
    if not yearly:
        return None

    company_by_rbukrs = {str(r.RBUKRS): r for r in company_rows}

    rows: List[dict[str, Any]] = []
    event_counter = 0
    for r in yearly:
        rev_y = float(r.rev_y) if r.rev_y is not None else 0.0
        op_y = float(r.op_y) if r.op_y is not None else 0.0
        if rev_y <= 0:
            continue  # can't compute margin without revenue
        current_margin = op_y / rev_y
        gap = trueup_adjustment(current_margin, lrd_target.om_low_pct, lrd_target.om_high_pct)
        if gap == 0.0:
            continue
        adjustment = Decimal(str(round(gap * rev_y, 2)))
        if adjustment == 0:
            continue

        lrd_row = company_by_rbukrs.get(str(r.RBUKRS))
        if lrd_row is None:
            continue
        awref = f"{_TRUEUP_AWREF_PREFIX}{_u32(seed + event_counter * 31):09d}"
        rows.extend(
            _trueup_je_rows(
                lrd_row=lrd_row,
                principal_row=principal,
                adjustment=adjustment,
                gjahr=gjahr,
                seed=seed,
                event_key=event_counter,
                group_currency=group_currency,
                awref=awref,
            )
        )
        event_counter += 1

    if not rows:
        return None
    return align_to_acdoca(spark.createDataFrame(rows, schema=sc_ic_document_schema()))
