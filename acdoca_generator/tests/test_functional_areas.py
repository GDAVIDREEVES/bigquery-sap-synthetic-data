"""Functional-area (FKBER / RFAREA) mapping tests."""

from __future__ import annotations

import re

from acdoca_generator.config.functional_areas import (
    FUNCTIONAL_AREAS,
    _KIND_DEFAULT,
    _ROLE_OVERRIDES,
    rfarea_for,
)
from acdoca_generator.config.operating_models import ROLE_BY_CODE


def test_functional_areas_codes_are_4char_strings() -> None:
    for code, desc in FUNCTIONAL_AREAS.items():
        assert isinstance(code, str)
        assert len(code) == 4
        assert re.fullmatch(r"\d{4}", code), f"{code!r} not 4-digit numeric"
        assert isinstance(desc, str) and desc, f"{code} has empty description"


def test_rfarea_for_known_defaults() -> None:
    for kind, expected in _KIND_DEFAULT.items():
        assert rfarea_for(kind, None) == expected


def test_rfarea_for_unknown_kind_returns_blank() -> None:
    assert rfarea_for("nonsense", "LRD") == ""
    assert rfarea_for("", "IPPR") == ""


def test_rfarea_for_unknown_role_falls_back_to_kind_default() -> None:
    # 'payroll' default is "0300"; ZZZ is not in ROLE_BY_CODE
    assert rfarea_for("payroll", "ZZZ") == "0300"


def test_rfarea_for_payroll_role_split() -> None:
    """Each role group maps payroll to the expected functional area."""
    production = ("TOLL", "CMFR", "LMFR", "FRMF", "IPLIC")
    sales      = ("LRD", "FFD", "COMM", "COMA", "BSDIST")
    rd         = ("RDSC", "IPDEV")
    admin      = ("IPPR", "RHQ", "SSC", "SSP", "RSP", "ENTR", "CPE", "FINC")

    for r in production: assert rfarea_for("payroll", r) == "0100", r
    for r in sales:      assert rfarea_for("payroll", r) == "0200", r
    for r in rd:         assert rfarea_for("payroll", r) == "0400", r
    for r in admin:      assert rfarea_for("payroll", r) == "0300", r


def test_rfarea_for_other_role_split() -> None:
    """`other` kind also varies by role flavor."""
    assert rfarea_for("other", "LRD") == "0500"
    assert rfarea_for("other", "FFD") == "0500"
    assert rfarea_for("other", "BSDIST") == "0500"
    assert rfarea_for("other", "COMA") == "0200"
    assert rfarea_for("other", "TOLL") == "0100"
    assert rfarea_for("other", "RDSC") == "0400"
    # default for un-overridden roles
    assert rfarea_for("other", "IPPR") == "0300"


def test_rfarea_for_depreciation_distributors_to_dist() -> None:
    for r in ("LRD", "FFD", "BSDIST"):
        assert rfarea_for("depreciation", r) == "0600", r


def test_role_overrides_keys_subset_of_kinds_and_roles() -> None:
    for (kind, role), code in _ROLE_OVERRIDES.items():
        assert kind in _KIND_DEFAULT, f"override kind {kind!r} not in _KIND_DEFAULT"
        assert role in ROLE_BY_CODE, f"override role {role!r} not in ROLE_BY_CODE"
        assert code in FUNCTIONAL_AREAS, f"override code {code!r} not in FUNCTIONAL_AREAS"


def test_kind_defaults_codes_are_known_or_blank() -> None:
    for kind, code in _KIND_DEFAULT.items():
        assert code == "" or code in FUNCTIONAL_AREAS, f"{kind!r} -> {code!r} not in FUNCTIONAL_AREAS"
