"""Complexity tiers and field coverage (SPEC Appendix A)."""

from __future__ import annotations

from ._field_specs_generated import FIELD_SPECS

TIER_ORDER = {"L": 0, "M": 1, "H": 2, "V": 3, "X": 4}

COMPLEXITY_LEVELS = ("light", "medium", "high", "very_high")

_COMPLEXITY_MAX_TIER = {
    "light": "L",
    "medium": "M",
    "high": "H",
    "very_high": "V",
}


def sql_name_for_sap(sap_name: str) -> str:
    for sap, sql, _t, _s in FIELD_SPECS:
        if sap == sap_name:
            return sql
    raise KeyError(sap_name)


def field_sql_names() -> list[str]:
    return [sql for _sap, sql, _t, _s in FIELD_SPECS]


def fields_for_complexity(complexity: str) -> frozenset[str]:
    """SQL column names populated (non-NULL) at this complexity; X fields never."""
    if complexity not in _COMPLEXITY_MAX_TIER:
        raise ValueError(f"Unknown complexity {complexity!r}")
    max_t = TIER_ORDER[_COMPLEXITY_MAX_TIER[complexity]]
    out: set[str] = set()
    for _sap, sql, tier, _stype in FIELD_SPECS:
        if tier == "X":
            continue
        if TIER_ORDER[tier] <= max_t:
            out.add(sql)
    return frozenset(out)


def excluded_sql_names() -> frozenset[str]:
    return frozenset(sql for _sap, sql, tier, _s in FIELD_SPECS if tier == "X")
