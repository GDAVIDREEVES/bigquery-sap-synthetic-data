"""Field-description plumbing: XLSX → schema metadata → DataFrame round-trip."""

from __future__ import annotations

import pytest

from acdoca_generator.config._field_descriptions_generated import FIELD_DESCRIPTIONS
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.utils.schema import acdoca_schema


def test_field_descriptions_module_known_keys() -> None:
    assert FIELD_DESCRIPTIONS["RBUKRS"] == "Company Code"
    assert FIELD_DESCRIPTIONS["GJAHR"] == "Fiscal Year"
    assert FIELD_DESCRIPTIONS["BUDAT"] == "Posting Date in the Document"
    assert len(FIELD_DESCRIPTIONS) >= 530


def test_acdoca_schema_carries_descriptions() -> None:
    schema = acdoca_schema()
    by_name = {f.name: f for f in schema.fields}
    assert by_name["RBUKRS"].metadata.get("description") == "Company Code"
    assert by_name["GJAHR"].metadata.get("description") == "Fiscal Year"
    assert by_name["WSL"].metadata.get("description") == "Amount in Transaction Currency"


def test_generated_dataframe_preserves_descriptions(spark) -> None:
    """End-to-end: descriptions survive the full pipeline (unions, _apply_tier, trueup)."""
    cfg = GenerationConfig(
        industry_key="consumer_goods",
        country_isos=["US", "DE"],
        fiscal_year=2026,
        fiscal_variant="calendar",
        complexity="light",
        txn_per_cc_per_period=20,
        ic_pct=0.25,
        include_reversals=False,
        include_closing=False,
        seed=11,
    )
    df = generate_acdoca_dataframe(spark, cfg).acdoca_df
    by_name = {f.name: f for f in df.schema.fields}
    assert by_name["RBUKRS"].metadata.get("description") == "Company Code"
    assert by_name["GJAHR"].metadata.get("description") == "Fiscal Year"
    assert by_name["BELNR"].metadata.get("description") == "Accounting Document Number"
