"""SAP Functional Area vocabulary and (kind, role) -> functional-area mapping.

The functional area classifies P&L lines by *function* — Production, Sales,
Administration, R&D, Marketing, Distribution — independent of the GL account.
Same GL (e.g. opex_sga = 0930000) can carry different functional-area values
across lines based on what the entity does and what the line is for.

In S/4HANA ACDOCA the column is ``RFAREA`` (the receiver-side functional area;
``SFAREA`` carries the sender side on IC documents). In ECC the field was
called ``FKBER``; the value codes (0100–0600 etc.) are the same.

For operational TP analytics, functional-area population is what lets you split:
- LRD payroll  -> Sales (rolls into S&M opex)
- IPPR payroll -> Administration (G&A)
- TOLL payroll -> Production (in COGS bucket)
- RDSC payroll -> R&D

Without it all of these lump into a single opex bucket and you can't run
role-margin diagnostics.

The mapping is keyed on (kind, role) where kind matches the transaction kinds
in transactions.MIX_GL_PAIRS (and a few synthetic kinds for IC / supply chain
legs that don't have a 1:1 transactions kind).
"""

from __future__ import annotations

from typing import Optional

# SAP standard functional area codes (4-character strings, leading zero).
FUNCTIONAL_AREAS: dict[str, str] = {
    "0100": "Production",
    "0200": "Sales",
    "0300": "Administration",
    "0400": "Research & Development",
    "0500": "Marketing",
    "0600": "Distribution",
}


# Default FKBER per kind; used when no role override applies.
_KIND_DEFAULT: dict[str, str] = {
    # transactions.MIX_GL_PAIRS kinds
    "third_party_revenue": "0200",
    "procurement_cogs":    "0100",
    "ic_goods":            "0100",
    "ic_services":         "0300",
    "rd":                  "0400",
    "tax":                 "0300",
    "depreciation":        "0300",
    "payroll":             "0300",
    "other":               "0300",
    # synthetic kinds for IC / supply-chain legs
    "ic_revenue":     "0200",
    "ic_cogs":        "0100",
    "ic_inventory":   "",
    "ic_payable":     "",
    "ic_receivable":  "",
    "closing_pl":     "0300",
}


# Role-conditioned overrides — only kinds whose function meaningfully varies by role.
_ROLE_OVERRIDES: dict[tuple[str, str], str] = {
    # payroll: function depends on what the entity does
    ("payroll", "TOLL"):   "0100", ("payroll", "CMFR"):  "0100",
    ("payroll", "LMFR"):   "0100", ("payroll", "FRMF"):  "0100", ("payroll", "IPLIC"): "0100",
    ("payroll", "LRD"):    "0200", ("payroll", "FFD"):   "0200",
    ("payroll", "COMM"):   "0200", ("payroll", "COMA"):  "0200", ("payroll", "BSDIST"): "0200",
    ("payroll", "RDSC"):   "0400", ("payroll", "IPDEV"): "0400",
    ("payroll", "IPPR"):   "0300", ("payroll", "RHQ"):   "0300", ("payroll", "SSC"):    "0300",
    ("payroll", "SSP"):    "0300", ("payroll", "RSP"):   "0300", ("payroll", "ENTR"):   "0300",
    ("payroll", "CPE"):    "0300", ("payroll", "FINC"):  "0300",
    # depreciation: aligns with where assets sit
    ("depreciation", "TOLL"):   "0100", ("depreciation", "CMFR"):  "0100",
    ("depreciation", "LMFR"):   "0100", ("depreciation", "FRMF"):  "0100", ("depreciation", "IPLIC"): "0100",
    ("depreciation", "LRD"):    "0600", ("depreciation", "FFD"):   "0600", ("depreciation", "BSDIST"): "0600",
    ("depreciation", "RDSC"):   "0400", ("depreciation", "IPDEV"): "0400",
    # ic_services: R&D-flavored entities receive R&D-coded services
    ("ic_services", "RDSC"):    "0400", ("ic_services", "IPDEV"):  "0400",
    # other: catch-all routed by role flavor
    ("other", "LRD"):     "0500", ("other", "FFD"):    "0500", ("other", "BSDIST"): "0500",
    ("other", "COMM"):    "0200", ("other", "COMA"):   "0200",
    ("other", "TOLL"):    "0100", ("other", "CMFR"):   "0100",
    ("other", "LMFR"):    "0100", ("other", "FRMF"):   "0100", ("other", "IPLIC"):  "0100",
    ("other", "RDSC"):    "0400", ("other", "IPDEV"):  "0400",
}


def rfarea_for(kind: str, role: Optional[str]) -> str:
    """Return the functional-area code for a (kind, role) pair.

    Output is intended for the ACDOCA ``RFAREA`` column (and ``SFAREA`` on the
    partner side). Returns empty string for unknown kind (SAP convention for
    non-functional lines). Falls back to the kind default when role is None
    or unknown.
    """
    if kind not in _KIND_DEFAULT:
        return ""
    if role and (kind, role) in _ROLE_OVERRIDES:
        return _ROLE_OVERRIDES[(kind, role)]
    return _KIND_DEFAULT[kind]
