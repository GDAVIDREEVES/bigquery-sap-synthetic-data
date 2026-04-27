"""Operating-model role taxonomy tests."""

from __future__ import annotations

from acdoca_generator.config.operating_models import DEFAULT_ROLES, ROLE_BY_CODE


def test_role_inventory_has_20_entries() -> None:
    """Existing 10 + 10 expanded TP roles."""
    assert len(DEFAULT_ROLES) == 20


def test_role_codes_unique() -> None:
    codes = [r.code for r in DEFAULT_ROLES]
    assert len(set(codes)) == len(codes)


def test_om_bands_valid() -> None:
    for r in DEFAULT_ROLES:
        assert 0.0 <= r.om_low_pct <= r.om_high_pct <= 1.0, (
            f"{r.code}: bad band [{r.om_low_pct}, {r.om_high_pct}]"
        )


def test_role_lookup_by_code() -> None:
    for code in ("LMFR", "FFD", "COMA", "BSDIST", "RSP", "SSP", "IPDEV", "IPLIC", "ENTR", "CPE"):
        assert code in ROLE_BY_CODE, f"new role {code} missing from ROLE_BY_CODE"
        assert ROLE_BY_CODE[code].code == code


def test_existing_roles_preserved() -> None:
    for code in ("IPPR", "FRMF", "CMFR", "TOLL", "LRD", "COMM", "RDSC", "SSC", "RHQ", "FINC"):
        assert code in ROLE_BY_CODE
