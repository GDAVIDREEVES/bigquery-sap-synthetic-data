"""Transfer pricing method hints by seller/buyer entity role pair."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TPMethod:
    code: str
    description: str
    markup_low: float
    markup_high: float


# (seller_role, buyer_role) -> method
ROLE_TP_METHOD: dict[tuple[str, str], TPMethod] = {
    ("TOLL", "IPPR"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.05),
    ("CMFR", "IPPR"): TPMethod("COST_PLUS", "Cost Plus", 0.05, 0.08),
    ("CMFR", "FRMF"): TPMethod("COST_PLUS", "Cost Plus", 0.04, 0.07),
    ("IPPR", "FRMF"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.01, 0.02),
    ("IPPR", "LRD"): TPMethod("RPM", "Resale Price Method", -0.04, -0.02),
    ("FRMF", "LRD"): TPMethod("RPM", "Resale Price Method", -0.04, -0.02),
    ("FRMF", "IPPR"): TPMethod("TNMM", "Transactional Net Margin Method", 0.02, 0.05),
    ("LRD", "LRD"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.01),
    ("RDSC", "IPPR"): TPMethod("COST_PLUS", "Cost Plus", 0.06, 0.10),
    ("SSC", "IPPR"): TPMethod("COST_PLUS", "Cost Plus", 0.05, 0.08),
    ("RHQ", "LRD"): TPMethod("PSM", "Profit Split Method", 0.0, 0.03),
    ("IPPR", "COMM"): TPMethod("RPM", "Resale Price Method", -0.03, -0.02),
    ("COMM", "LRD"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.0),
    ("FINC", "IPPR"): TPMethod("TNMM", "Transactional Net Margin Method", 0.0, 0.02),
    ("IPPR", "FINC"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.01, 0.01),
    ("TOLL", "FRMF"): TPMethod("COST_PLUS", "Cost Plus", 0.03, 0.06),
    ("LRD", "IPPR"): TPMethod("CUP", "Comparable Uncontrolled Price", -0.02, 0.0),
}

_DEFAULT_TP = TPMethod("TNMM", "Transactional Net Margin Method (default)", 0.02, 0.06)


def tp_method_for_roles(seller_role: str, buyer_role: str) -> TPMethod:
    return ROLE_TP_METHOD.get((seller_role, buyer_role), _DEFAULT_TP)
