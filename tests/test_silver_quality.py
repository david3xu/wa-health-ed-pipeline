"""
Data quality tests for the WA Health ED Pipeline silver and gold layers.

These tests validate:
- Value ranges are within expected bounds
- Required tables are non-empty
- WA hospital data is present
- No critical null values
"""

import pytest
from pyspark.sql.functions import col


# ----------------------------------------------------------------
# Silver layer tests — fact_ed_performance
# ----------------------------------------------------------------

def test_ed_performance_value_range(spark):
    """MYH0005 (4-hour departure rate) must be a valid percentage 0-100."""
    df = spark.table("silver.fact_ed_performance")
    invalid = df.filter(
        (col("measure_code") == "MYH0005") &
        ((col("value") < 0) | (col("value") > 100))
    ).count()
    assert invalid == 0, f"{invalid} rows have 4-hour departure rate outside 0-100%"


def test_ed_performance_not_empty(spark):
    """Silver ED performance table must have rows."""
    df = spark.table("silver.fact_ed_performance")
    assert df.count() > 0, "silver.fact_ed_performance is empty"


def test_ed_performance_has_wa_hospitals(spark):
    """At least one WA hospital must be present in the silver table."""
    df = spark.table("silver.fact_ed_performance")
    hospital_count = df.select("hospital_code").distinct().count()
    assert hospital_count > 0, "No WA hospitals found in silver.fact_ed_performance"


def test_ed_performance_no_null_hospital_codes(spark):
    """Hospital codes must not be null."""
    df = spark.table("silver.fact_ed_performance")
    nulls = df.filter(col("hospital_code").isNull()).count()
    assert nulls == 0, f"{nulls} rows have null hospital_code"


def test_ed_performance_no_null_dates(spark):
    """Time period start must not be null."""
    df = spark.table("silver.fact_ed_performance")
    nulls = df.filter(col("time_period_start").isNull()).count()
    assert nulls == 0, f"{nulls} rows have null time_period_start"


def test_ed_performance_all_measures_present(spark):
    """All four expected measure codes must be present."""
    expected = {"MYH0005", "MYH0010", "MYH0011", "MYH0013"}
    df = spark.table("silver.fact_ed_performance")
    actual = {r.measure_code for r in df.select("measure_code").distinct().collect()}
    missing = expected - actual
    assert len(missing) == 0, f"Missing measure codes: {missing}"


# ----------------------------------------------------------------
# Silver layer tests — dim_hospitals
# ----------------------------------------------------------------

def test_hospital_dimension_not_empty(spark):
    """Hospital dimension table must have rows."""
    df = spark.table("silver.dim_hospitals")
    assert df.count() > 0, "silver.dim_hospitals is empty"


def test_hospital_dimension_has_health_services(spark):
    """At least some hospitals must have a health_service assigned."""
    df = spark.table("silver.dim_hospitals")
    with_service = df.filter(col("health_service").isNotNull()).count()
    assert with_service > 0, "No hospitals have a health_service value"


def test_hospital_coordinates_in_wa(spark):
    """WA is approx longitude 113-130, latitude -35 to -13. Check bounds."""
    df = spark.table("silver.dim_hospitals")
    out_of_bounds = df.filter(
        (col("longitude").isNotNull()) &
        (col("latitude").isNotNull()) &
        (
            (col("longitude") < 113) | (col("longitude") > 130) |
            (col("latitude") < -35) | (col("latitude") > -13)
        )
    ).count()
    assert out_of_bounds == 0, f"{out_of_bounds} hospitals have coordinates outside WA bounds"


# ----------------------------------------------------------------
# Gold layer tests — ed_waittime_trends
# ----------------------------------------------------------------

def test_gold_table_not_empty(spark):
    """Gold ED trends table must have rows."""
    df = spark.table("gold.ed_waittime_trends")
    assert df.count() > 0, "gold.ed_waittime_trends is empty"


def test_gold_wa_average_in_valid_range(spark):
    """WA average 4-hour rate must be 0-100."""
    df = spark.table("gold.ed_waittime_trends")
    invalid = df.filter(
        (col("wa_average") < 0) | (col("wa_average") > 100)
    ).count()
    assert invalid == 0, f"{invalid} rows have wa_average outside valid range"


def test_gold_has_below_target_flag(spark):
    """below_target column must not be all null."""
    df = spark.table("gold.ed_waittime_trends")
    non_null = df.filter(col("below_target").isNotNull()).count()
    assert non_null > 0, "below_target column is entirely null"
