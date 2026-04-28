"""Entity-roles master-file metadata tests."""

from __future__ import annotations

import pytest

from acdoca_generator.aggregations.entity_roles import (
    FLAG_COLUMNS,
    _FLAG_MEMBERS,
    role_flags_for,
)
from acdoca_generator.config.operating_models import DEFAULT_ROLES, ROLE_BY_CODE


# ---------------------------------------------------------------------------
# Fast (non-spark) tests — flag taxonomy is pure-Python
# ---------------------------------------------------------------------------


def test_principal_flag_set_for_ippr_and_entr() -> None:
    assert role_flags_for("IPPR")["IS_PRINCIPAL"] is True
    assert role_flags_for("ENTR")["IS_PRINCIPAL"] is True
    assert role_flags_for("LRD")["IS_PRINCIPAL"] is False


def test_ip_owner_flag_excludes_licensee() -> None:
    """IPLIC is the licensee, not the owner — flag must stay False."""
    assert role_flags_for("IPPR")["IS_IP_OWNER"] is True
    assert role_flags_for("IPDEV")["IS_IP_OWNER"] is True
    assert role_flags_for("IPLIC")["IS_IP_OWNER"] is False


def test_financing_arm_flag_only_for_finc() -> None:
    assert role_flags_for("FINC")["IS_FINANCING_ARM"] is True
    for code in ROLE_BY_CODE:
        if code == "FINC":
            continue
        assert role_flags_for(code)["IS_FINANCING_ARM"] is False


def test_distributor_flag_covers_all_distribution_roles() -> None:
    for code in ("LRD", "FFD", "BSDIST", "COMM", "COMA"):
        assert role_flags_for(code)["IS_DISTRIBUTOR"] is True, code


def test_full_risk_mfr_flag_covers_frmf_and_lmfr() -> None:
    assert role_flags_for("FRMF")["IS_FULL_RISK_MFR"] is True
    assert role_flags_for("LMFR")["IS_FULL_RISK_MFR"] is True
    assert role_flags_for("CMFR")["IS_FULL_RISK_MFR"] is False
    assert role_flags_for("TOLL")["IS_FULL_RISK_MFR"] is False


def test_service_provider_flag_covers_service_roles() -> None:
    for code in ("RDSC", "SSC", "RSP", "SSP"):
        assert role_flags_for(code)["IS_SERVICE_PROVIDER"] is True, code


def test_unknown_role_code_returns_all_false_flags() -> None:
    flags = role_flags_for("ZZZZ")
    assert all(v is False for v in flags.values())
    assert set(flags.keys()) == set(FLAG_COLUMNS)


def test_flag_columns_match_flag_members_keys() -> None:
    """Public FLAG_COLUMNS tuple stays in sync with the internal taxonomy dict."""
    assert FLAG_COLUMNS == tuple(_FLAG_MEMBERS.keys())


def test_flag_members_only_reference_known_role_codes() -> None:
    """No flag may name a role code that doesn't exist in the catalog."""
    known = {r.code for r in DEFAULT_ROLES}
    for flag, members in _FLAG_MEMBERS.items():
        unknown = members - known
        assert not unknown, f"{flag} references unknown role codes: {unknown}"


def test_every_role_in_catalog_has_at_least_one_flag_or_is_intentionally_unflagged() -> None:
    """Sanity: each role code resolves to a flag dict of the right shape, even if all-False."""
    for code in ROLE_BY_CODE:
        flags = role_flags_for(code)
        assert set(flags.keys()) == set(FLAG_COLUMNS), code


# ---------------------------------------------------------------------------
# Spark integration test — gated by the conftest auto-marker
# ---------------------------------------------------------------------------


def test_build_entity_roles_one_row_per_bukrs(spark) -> None:
    from acdoca_generator.aggregations.entity_roles import build_entity_roles
    from acdoca_generator.generators.master_data import build_companies

    companies = build_companies(spark, ["US", "DE", "CH", "IE"], "pharmaceutical", _seed=7)
    out = build_entity_roles(companies)
    rows = out.collect()
    assert len(rows) == 4
    bukrs = {r.RBUKRS for r in rows}
    assert len(bukrs) == 4  # PK uniqueness

    # Pharma override: US → IPPR (principal + IP owner), IE → FINC (financing arm)
    by_country = {r.LAND1: r for r in rows}
    assert by_country["US"].ROLE_CODE == "IPPR"
    assert by_country["US"].IS_PRINCIPAL is True
    assert by_country["US"].IS_IP_OWNER is True
    assert by_country["IE"].ROLE_CODE == "FINC"
    assert by_country["IE"].IS_FINANCING_ARM is True

    # Margin band carried through
    assert by_country["US"].OM_LOW_PCT == pytest.approx(0.30)
    assert by_country["US"].OM_HIGH_PCT == pytest.approx(0.45)


def test_build_entity_roles_schema(spark) -> None:
    from acdoca_generator.aggregations.entity_roles import FLAG_COLUMNS, build_entity_roles
    from acdoca_generator.generators.master_data import build_companies

    companies = build_companies(spark, ["US", "DE"], "pharmaceutical", _seed=1)
    out = build_entity_roles(companies)
    cols = out.columns
    expected_prefix = ["RBUKRS", "LAND1", "ROLE_CODE", "ROLE_DESCRIPTION", "OM_LOW_PCT", "OM_HIGH_PCT"]
    assert cols[:6] == expected_prefix
    assert set(cols[6:]) == set(FLAG_COLUMNS)
