from acdoca_generator.config.field_tiers import fields_for_complexity
from acdoca_generator.utils.schema import acdoca_schema


def test_tier_subset_and_schema_width():
    light = fields_for_complexity("light")
    med = fields_for_complexity("medium")
    high = fields_for_complexity("high")
    vh = fields_for_complexity("very_high")
    assert light <= med <= high <= vh
    assert "INCLU_PN" not in vh
    schema = acdoca_schema()
    assert len(schema.fields) == 538
