from src.pyspark_app.main import DataProcessor
import pyspark
from chispa import assert_df_equality
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat, col, lit
import sys

dp = DataProcessor()

# Test Data
data_for_test = [(1, "PL", 101),
                 (2, "GB", 102),
                 (3, "USA", 103),
                 (4, "CAN", 104),
                 (5, "FIN", 105),
                 (6, "RUS", 106),
                 (7, "UA", 107),
                 (8, "CZ", 108),
                 (9, "JPN", 109),
                 (10, "KOR", 110)]
columns_for_test = ["id", "attribute1", "attribute2"]

def load_dataframe(spark_session) -> DataFrame:
    """
    Load a PySpark DataFrame with test data.

    :param spark_session: A PySpark SparkSession.
    :return: A PySpark DataFrame.
    """
    return spark_session.createDataFrame(data=data_for_test, schema=columns_for_test)

def test_filter_data(spark_session):
    """
    Test the dataset_filtering method of DataProcessor.

    :param spark_session: A PySpark SparkSession.
    """
    expected_data = [(1, "PL", 101),
                     (10, "KOR", 110)]
    expected_columns = ["id", "attribute1", "attribute2"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(dp.dataset_filtering(load_dataframe(spark_session), "id in (1, 10)"), df_expected)

def test_rename_columns(spark_session):
    """
    Test the column_rename method of DataProcessor.

    :param spark_session: A PySpark SparkSession.
    """
    expected_data = [(1, "PL", 101),
                     (2, "GB", 102),
                     (3, "USA", 103),
                     (4, "CAN", 104),
                     (5, "FIN", 105),
                     (6, "RUS", 106),
                     (7, "UA", 107),
                     (8, "CZ", 108),
                     (9, "JPN", 109),
                     (10, "KOR", 110)]
    expected_columns = ["id", "attribute3", "attribute2"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(dp.column_rename(load_dataframe(spark_session), {'attribute1': 'attribute3'}), df_expected)

def test_join_datasets(spark_session):
    """
    Test the dataset_join method of DataProcessor.

    :param spark_session: A PySpark SparkSession.
    """
    data1_for_join = [(101, "00101"), (102, "00102"), (103, "00103"), (104, "00104"), (105, "00105")]
    columns1_for_join = ["id", "attribute10"]
    data2_for_join = [(101, 10100), (102, 10200), (105, 10500)]
    columns2_for_join = ["id", "attribute11"]

    expected_data = [(101, "00101", 10100),
                     (102, "00102", 10200),
                     (105, "00105", 10500)]
    expected_columns = ["id", "attribute10", "attribute11"]

    df_users = spark_session.createDataFrame(data=data1_for_join, schema=columns1_for_join)
    df_transactions = spark_session.createDataFrame(data=data2_for_join, schema=columns2_for_join)
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(dp.dataset_join(df_users, df_transactions, 'id'), df_expected)
