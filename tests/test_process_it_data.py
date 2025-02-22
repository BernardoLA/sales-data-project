from chispa.dataframe_comparer import assert_df_equality
from unittest.mock import patch
from sales_data.processing import ProcessOutputs
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    # Set up a Spark session for testing
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


def test_process_it_data(spark: SparkSession):
    # Create test DataFrames
    df_data = [
        (1, "Alice", "2588 VD, Kropswolde", 5000.0, "IT", 50, 30),
        (2, "Bob", "1808 KR, Benningbroek", 4500.0, "IT", 40, 20),
        (3, "Charlie", "Thijmenweg 38, 7801 OC, Grijpskerk", 6000.0, "IT", 60, 40),
        (4, "John", "Janlaan 4, 3319 GW, Rijs", 7000.0, "Sales", 20, 10),
    ]
    df_columns = [
        "id",
        "name",
        "address",
        "sales_amount",
        "area",
        "calls_made",
        "calls_successful",
    ]
    df = spark.createDataFrame(df_data, df_columns)

    expected_data = [
        ("Thijmenweg 38, 7801 OC, Grijpskerk", "Charlie", 3, 6000.0, "IT", 60, 40),
        ("2588 VD, Kropswolde", "Alice", 1, 5000.0, "IT", 50, 30),
        ("1808 KR, Benningbroek", "Bob", 2, 4500.0, "IT", 40, 20),
    ]
    expected_columns = [
        "address",
        "name",
        "id",
        "sales_amount",
        "area",
        "calls_made",
        "calls_successful",
    ]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Mock write_csv to capture the DataFrame passed to it
    with patch("sales_data.processing.write_csv") as mock_write:
        ProcessOutputs.process_it_data(df, "dummy path")
        # Ensure write_csv was called once and check the arguments passed
        mock_write.assert_called_once()

        # Get the actual DataFrame passed to write_csv
        actual_df = mock_write.call_args[0][0]

        # Compare actual vs expected DataFrame
        assert_df_equality(
            actual_df, expected_df, ignore_nullable=True, ignore_column_order=True
        )
