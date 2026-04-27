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
    # ----- Expanded TP role taxonomy (manufacturing, distribution, services, IP, principal/entrepreneur, hybrid) -----
    EntityRole(
        "LMFR",
        "Licensed Manufacturer",
        frozenset({"IN", "MX", "BR", "ID"}),
        0.08,
        0.15,
    ),
    EntityRole(
        "FFD",
        "Fully-Fledged Distributor",
        frozenset({"US", "GB", "DE", "FR", "JP"}),
        0.05,
        0.10,
    ),
    EntityRole(
        "COMA",
        "Commission Agent / Sales Agent",
        frozenset({"FR", "IT", "ES"}),
        0.01,
        0.03,
    ),
    EntityRole(
        "BSDIST",
        "Buy-Sell Distributor",
        frozenset({"BR", "IN", "CN", "MX"}),
        0.03,
        0.06,
    ),
    EntityRole(
        "RSP",
        "Routine Service Provider",
        frozenset({"IN", "PH", "PL", "CR"}),
        0.05,
        0.08,
    ),
    EntityRole(
        "SSP",
        "Strategic Service Provider",
        frozenset({"US", "CH", "GB"}),
        0.10,
        0.18,
    ),
    EntityRole(
        "IPDEV",
        "IP Developer",
        frozenset({"US", "IL", "IN"}),
        0.05,
        0.10,
    ),
    EntityRole(
        "IPLIC",
        "IP Licensee",
        frozenset({"BR", "IN", "CN", "RU"}),
        0.05,
        0.12,
    ),
    EntityRole(
        "ENTR",
        "Entrepreneur",
        frozenset({"US", "DE", "JP"}),
        0.20,
        0.40,
    ),
    EntityRole(
        "CPE",
        "Central Purchasing Entity",
        frozenset({"SG", "NL", "CH"}),
        0.03,
        0.05,
    ),
)

ROLE_BY_CODE = {r.code: r for r in DEFAULT_ROLES}

# Optional per-industry overrides (canonical industry keys). Unlisted countries fall back to DEFAULT_ROLES scan.
INDUSTRY_COUNTRY_ROLE: dict[str, dict[str, str]] = {
    "technology": {"US": "IPPR", "IN": "RDSC", "DE": "LRD", "GB": "RHQ", "IL": "RDSC"},
    "pharmaceutical": {"US": "IPPR", "CH": "IPPR", "IE": "FINC", "DE": "FRMF", "IN": "TOLL"},
    "medical_device": {"US": "IPPR", "DE": "FRMF", "IN": "CMFR"},
    "consumer_goods": {"US": "LRD", "DE": "LRD", "GB": "LRD"},
    "media": {"US": "IPPR", "GB": "LRD", "FR": "COMM"},
}


def pick_role_for_country(iso: str, industry_key: str | None = None) -> EntityRole:
    """Resolve entity role: industry-specific map first, else first DEFAULT_ROLES match; else LRD."""
    if industry_key:
        from acdoca_generator.config.industries import canonical_industry_key

        ck = canonical_industry_key(industry_key)
        ov = INDUSTRY_COUNTRY_ROLE.get(ck)
        if ov:
            code = ov.get(iso)
            if code and code in ROLE_BY_CODE:
                return ROLE_BY_CODE[code]
    for r in DEFAULT_ROLES:
        if iso in r.typical_countries_iso:
            return r
    return ROLE_BY_CODE["LRD"]
