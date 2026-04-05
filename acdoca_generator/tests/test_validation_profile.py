from pyspark.sql import DataFrame

from acdoca_generator.validators.balance import blocking_failures, run_validations


def test_fast_profile_skips_pk_full_scan(shared_pharma_df: DataFrame):
    results = run_validations(shared_pharma_df, profile="fast")
    pk = next(r for r in results if r.name == "PK_UNIQUE")
    assert pk.passed
    assert pk.metrics.get("skipped") is True
    assert blocking_failures(results) == []
