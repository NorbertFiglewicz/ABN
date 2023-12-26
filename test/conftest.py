import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
