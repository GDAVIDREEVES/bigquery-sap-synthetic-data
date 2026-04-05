from pyspark.sql import DataFrame

from acdoca_generator.validators.balance import run_validations


def test_doc_balance_and_pk(shared_pharma_df: DataFrame):
    results = run_validations(shared_pharma_df)
    by_name = {r.name: r for r in results}
    assert by_name["DOC_BALANCE"].passed, by_name["DOC_BALANCE"].detail
    assert by_name["PK_UNIQUE"].passed, by_name["PK_UNIQUE"].detail
    assert by_name["DRCRK_SIGN"].passed, by_name["DRCRK_SIGN"].detail
