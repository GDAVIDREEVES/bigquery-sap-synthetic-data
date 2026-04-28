"""Transfer pricing method hints by (seller_role, buyer_role, transaction_type).

The third tuple element discriminates flow types so the same role pair can
use different methods for goods vs. services vs. royalties (a real TP study
documents per-transaction-type method selection, not per-role-pair).

Lookup falls back to "goods" when a 3-tuple miss occurs, then to a default
TNMM. Existing call sites that don't pass `transaction_type` get "goods".
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TPMethod:
    code: str
    description: str
    markup_low: float
    markup_high: float


# (seller_role, buyer_role, transaction_type) -> method
# transaction_type ∈ {"goods", "royalty", "service", "cost_share"}.
ROLE_TP_METHOD: dict[tuple[str, str, str], TPMethod] = {
    # ----- Goods flows (existing role-pair hints) -----
    ("TOLL", "IPPR", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.05),
    ("CMFR", "IPPR", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.05, 0.08),
    ("CMFR", "FRMF", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.04, 0.07),
    ("IPPR", "FRMF", "goods"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.01, 0.02),
    ("IPPR", "LRD", "goods"): TPMethod("RPM", "Resale Price Method", -0.04, -0.02),
    ("FRMF", "LRD", "goods"): TPMethod("RPM", "Resale Price Method", -0.04, -0.02),
    ("FRMF", "IPPR", "goods"): TPMethod("TNMM", "Transactional Net Margin Method", 0.02, 0.05),
    ("LRD", "LRD", "goods"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.01),
    ("RDSC", "IPPR", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.06, 0.10),
    ("SSC", "IPPR", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.05, 0.08),
    ("RHQ", "LRD", "goods"): TPMethod("PSM", "Profit Split Method", 0.0, 0.03),
    ("IPPR", "COMM", "goods"): TPMethod("RPM", "Resale Price Method", -0.03, -0.02),
    ("COMM", "LRD", "goods"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.0),
    ("FINC", "IPPR", "goods"): TPMethod("TNMM", "Transactional Net Margin Method", 0.0, 0.02),
    ("IPPR", "FINC", "goods"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.01, 0.01),
    ("TOLL", "FRMF", "goods"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.06),
    ("LRD", "IPPR", "goods"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.0),

    # ----- Royalty / IP licensing -----
    # markup_low/high here are interpreted as royalty rates applied to a base revenue.
    ("IPPR", "FRMF", "royalty"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.04, 0.07),
    ("IPPR", "LRD", "royalty"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.05, 0.08),
    ("IPDEV", "IPPR", "royalty"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.03, 0.06),
    ("IPPR", "IPLIC", "royalty"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.05, 0.10),

    # ----- Service charges -----
    # Cost-plus on a service cost base; markup is the cost-plus uplift.
    ("RHQ", "LRD", "service"): TPMethod("COST_PLUS", "Cost Plus", 0.04, 0.07),
    ("RHQ", "FRMF", "service"): TPMethod("COST_PLUS", "Cost Plus", 0.04, 0.07),
    ("SSC", "IPPR", "service"): TPMethod("COST_PLUS", "Cost Plus", 0.05, 0.08),
    ("SSC", "RHQ", "service"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.06),
    ("SSC", "LRD", "service"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.06),

    # ----- Cost-share recharges -----
    # Recharge of pooled R&D costs at-cost to participants (CUP at zero markup).
    ("RDSC", "IPPR", "cost_share"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.0, 0.0),
    ("RDSC", "FRMF", "cost_share"): TPMethod("CUP", "Comparable Uncontrolled Price", 0.0, 0.0),
}

_DEFAULT_TP = TPMethod("TNMM", "Transactional Net Margin Method (default)", 0.02, 0.06)


def tp_method_for_roles(
    seller_role: str,
    buyer_role: str,
    transaction_type: str = "goods",
) -> TPMethod:
    """Resolve TP method for a flow.

    Lookup order: (seller, buyer, transaction_type) → (seller, buyer, "goods") → default.
    The "goods" fallback keeps backward compat for any 3-tuple that isn't yet mapped.
    """
    hit = ROLE_TP_METHOD.get((seller_role, buyer_role, transaction_type))
    if hit is not None:
        return hit
    if transaction_type != "goods":
        hit = ROLE_TP_METHOD.get((seller_role, buyer_role, "goods"))
        if hit is not None:
            return hit
    return _DEFAULT_TP
