from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def test_ic_awref_pairs_two_company_codes(shared_pharma_df: DataFrame):
    ic = shared_pharma_df.filter(F.col("RASSC") != "")
    if ic.limit(1).count() == 0:
        return
    paired = (
        ic.groupBy("AWREF")
        .agg(F.countDistinct("RBUKRS").alias("cc"))
        .filter(F.col("cc") >= 2)
        .count()
    )
    aw = ic.select("AWREF").distinct().count()
    assert paired == aw
