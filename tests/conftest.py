"""
Pytest configuration for WA Health ED Pipeline tests.

For local testing without a Fabric cluster, tests use a local
SparkSession with Delta Lake support.

Install dependencies:
    pip install pyspark delta-spark pytest
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta Lake support."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("wa-health-ed-pipeline-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
