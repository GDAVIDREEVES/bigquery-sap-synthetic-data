"""Currency amounts and FX multipliers (SPEC §6.4)."""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Tuple


# Mid-market style USD per 1 unit of foreign (simplified baseline).
_BASE_USD_PER_UNIT: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.09,
    "GBP": 1.27,
    "CHF": 1.13,
    "JPY": 0.0067,
    "CNY": 0.14,
    "INR": 0.012,
    "BRL": 0.20,
    "CAD": 0.74,
    "KRW": 0.00075,
    "DKK": 0.146,
    "SGD": 0.74,
    "ILS": 0.27,
    "AUD": 0.65,
}


def _u32(x: int) -> int:
    return x & 0xFFFFFFFF


def _mix(seed: int, a: int, b: int) -> float:
    """Deterministic [0,1) from seed and two integers."""
    h = _u32(seed ^ _u32(a * 0x9E3779B1) ^ _u32(b * 0x85EBCA6B))
    return (h % 1_000_000) / 1_000_000.0


def fx_multiplier(local_ccy: str, group_ccy: str, seed: int, month: int) -> float:
    """Small month-level noise on top of baseline USD cross-rate."""
    if local_ccy == group_ccy:
        return 1.0
    lu = _BASE_USD_PER_UNIT.get(local_ccy, 1.0)
    gu = _BASE_USD_PER_UNIT.get(group_ccy, 1.0)
    base = lu / gu
    wobble = 1.0 + (_mix(seed, hash(local_ccy) % 10000, month) - 0.5) * 0.04
    return base * wobble


def round_money(v: float) -> Decimal:
    return Decimal(str(round(v, 2)))


def hsl_ksl_pair(
    wsl_abs: float,
    rwcur: str,
    rhcur: str,
    rkcur: str,
    seed: int,
    month: int,
) -> Tuple[Decimal, Decimal]:
    """Assume WSL is in transaction currency RWCUR; derive HSL in local and KSL in group."""
    if rwcur == rhcur:
        hsl_f = wsl_abs
    else:
        hsl_f = wsl_abs * fx_multiplier(rwcur, rhcur, seed, month)
    if rhcur == rkcur:
        ksl_f = hsl_f
    else:
        ksl_f = hsl_f * fx_multiplier(rhcur, rkcur, seed, month)
    return round_money(hsl_f), round_money(ksl_f)
