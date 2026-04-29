"""Country reference data (SPEC §6). UK stored as ISO 3166-1 alpha-2 GB."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Country:
    display_name: str
    iso: str
    currency: str
    fy_variant_cal: str
    fy_variant_apr: Optional[str]
    tax_prefix: str
    sample_bukrs: int


# FY variants and sample company codes from spec; China Apr variant "—" -> None
COUNTRIES: tuple[Country, ...] = (
    Country("United States", "US", "USD", "K4", "V3", "US", 1000),
    Country("China", "CN", "CNY", "K4", None, "CN", 2000),
    Country("Germany", "DE", "EUR", "K4", None, "DE", 3000),
    Country("Switzerland", "CH", "CHF", "K4", None, "CH", 3100),
    Country("Japan", "JP", "JPY", "K4", "V6", "JP", 4000),
    Country("India", "IN", "INR", "K4", "V3", "IN", 4100),
    Country("France", "FR", "EUR", "K4", None, "FR", 3200),
    Country("United Kingdom", "GB", "GBP", "K4", "V9", "GB", 3300),
    Country("Ireland", "IE", "EUR", "K4", None, "IE", 3400),
    Country("Brazil", "BR", "BRL", "K4", None, "BR", 5000),
    Country("Canada", "CA", "CAD", "K4", None, "CA", 1100),
    Country("Belgium", "BE", "EUR", "K4", None, "BE", 3500),
    Country("Italy", "IT", "EUR", "K4", None, "IT", 3600),
    Country("South Korea", "KR", "KRW", "K4", None, "KR", 4200),
    Country("Spain", "ES", "EUR", "K4", None, "ES", 3700),
    Country("Netherlands", "NL", "EUR", "K4", None, "NL", 3800),
    Country("Denmark", "DK", "DKK", "K4", None, "DK", 3900),
    Country("Singapore", "SG", "SGD", "K4", None, "SG", 4300),
    Country("Israel", "IL", "ILS", "K4", None, "IL", 4400),
    Country("Australia", "AU", "AUD", "K4", "V3", "AU", 6000),
    Country("Mexico", "MX", "MXN", "K4", None, "MX", 5100),
    Country("Poland", "PL", "PLN", "K4", None, "PL", 3950),
    Country("Philippines", "PH", "PHP", "K4", None, "PH", 4500),
    Country("Costa Rica", "CR", "CRC", "K4", None, "CR", 5200),
    Country("Indonesia", "ID", "IDR", "K4", None, "ID", 4600),
    Country("Russia", "RU", "RUB", "K4", None, "RU", 5300),
)

_ISO_MAP = {c.iso: c for c in COUNTRIES}


def get_country(iso: str) -> Country:
    return _ISO_MAP[iso]
