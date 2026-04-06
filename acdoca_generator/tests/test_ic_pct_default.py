from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def test_ic_pct_none_uses_industry_default(tech_ic_default_df: DataFrame, pharma_ic_default_df: DataFrame):
    ic_t = tech_ic_default_df.filter(F.col("RASSC") != "").count()
    ic_p = pharma_ic_default_df.filter(F.col("RASSC") != "").count()
    assert ic_t > ic_p, f"technology default IC share > pharma: {ic_t} vs {ic_p}"
