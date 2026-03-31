"""Entity role archetypes and margin hints (SPEC §5)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet


@dataclass(frozen=True)
class EntityRole:
    code: str
    description: str
    typical_countries_iso: FrozenSet[str]
    om_low_pct: float
    om_high_pct: float


DEFAULT_ROLES: tuple[EntityRole, ...] = (
    EntityRole(
        "IPPR",
        "IP Principal",
        frozenset({"US", "CH", "IE", "SG"}),
        0.30,
        0.45,
    ),
    EntityRole(
        "FRMF",
        "Full-Risk Manufacturer",
        frozenset({"US", "DE", "JP"}),
        0.15,
        0.25,
    ),
    EntityRole(
        "CMFR",
        "Contract Manufacturer",
        frozenset({"IN", "IE", "SG", "CN"}),
        0.05,
        0.08,
    ),
    EntityRole(
        "TOLL",
        "Toll Manufacturer",
        frozenset({"IN", "CN", "BR"}),
        0.03,
        0.05,
    ),
    EntityRole(
        "LRD",
        "Limited Risk Distributor",
        frozenset({"DE", "FR", "GB", "IT", "ES", "NL", "KR", "AU", "BE", "DK", "CA"}),
        0.02,
        0.04,
    ),
    EntityRole(
        "COMM",
        "Commissionaire",
        frozenset({"FR", "IT", "BE"}),
        0.02,
        0.03,
    ),
    EntityRole(
        "RDSC",
        "R&D Service Center",
        frozenset({"IN", "CN", "IL"}),
        0.08,
        0.12,
    ),
    EntityRole(
        "SSC",
        "Shared Service Center",
        frozenset({"IN"}),
        0.05,
        0.08,
    ),
    EntityRole(
        "RHQ",
        "Regional HQ / Mgmt Co",
        frozenset({"CH", "SG", "GB", "US"}),
        0.05,
        0.10,
    ),
    EntityRole(
        "FINC",
        "Financing Entity",
        frozenset({"IE", "NL", "CH"}),
        0.0,
        0.0,
    ),
)

ROLE_BY_CODE = {r.code: r for r in DEFAULT_ROLES}


def pick_role_for_country(iso: str) -> EntityRole:
    """First role that lists the country; else LRD."""
    for r in DEFAULT_ROLES:
        if iso in r.typical_countries_iso:
            return r
    return ROLE_BY_CODE["LRD"]
