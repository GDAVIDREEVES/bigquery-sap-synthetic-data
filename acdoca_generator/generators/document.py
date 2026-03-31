"""Document / line numbering helpers (SPEC §7.1)."""

from __future__ import annotations


def format_belnr(seq: int) -> str:
    return str(seq % 10_000_000_000).zfill(10)


def format_docln(line: int) -> str:
    return str(line).zfill(6)


def format_buzei(line: int) -> str:
    return str(line).zfill(3)


def fiscyearper(gjahr: int, poper_int: int) -> int:
    return gjahr * 1000 + poper_int
