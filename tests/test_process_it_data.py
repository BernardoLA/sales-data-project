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
    emp_exp_calls_data = [
        (1, "IT", 50, 30),
        (2, "IT", 40, 20),
        (3, "IT", 60, 40),
        (4, "Sales", 70, 50),  # This should be filtered out
    ]
    emp_exp_calls_columns = ["id", "area", "calls_made", "calls_successful"]
    df_emp_exp_calls = spark.createDataFrame(emp_exp_calls_data, emp_exp_calls_columns)

    emp_per_sales_data = [
        (1, "Alice", "2588 VD, Kropswolde", 5000.0),
        (2, "Bob", "1808 KR, Benningbroek", 4500.0),
        (3, "Charlie", "Thijmenweg 38, 7801 OC, Grijpskerk", 6000.0),
        (4, "David", "4273 SW, Wirdum Gn", 7000.0),  # This should be filtered out
    ]
    emp_per_sales_columns = ["emp_id", "name", "address", "sales_amount"]
    df_emp_per_sales = spark.createDataFrame(emp_per_sales_data, emp_per_sales_columns)

    # Expected output DataFrame (filtered for "IT", sorted by sales_amount, top 100)
    expected_data = [
        (3, "Charlie", "Thijmenweg 38, 7801 OC, Grijpskerk", 6000.0, "IT", 60, 40),
        (1, "Alice", "2588 VD, Kropswolde", 5000.0, "IT", 50, 30),
        (2, "Bob", "1808 KR, Benningbroek", 4500.0, "IT", 40, 20),
    ]
    expected_columns = [
        "id",
        "name",
        "address",
        "sales_amount",
        "area",
        "calls_made",
        "calls_successful",
    ]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Mock write_csv to capture the DataFrame passed to it
    with patch("sales_data.processing.write_csv") as mock_write:
        process_outputs = ProcessOutputs(df_emp_exp_calls, df_emp_per_sales)
        process_outputs.process_it_data("dummy path")
        # Ensure write_csv was called once and check the arguments passed
        mock_write.assert_called_once()

        # Get the actual DataFrame passed to write_csv
        actual_df = mock_write.call_args[0][0]

        # Compare actual vs expected DataFrame
        assert_df_equality(
            actual_df, expected_df, ignore_nullable=True, ignore_column_order=True
        )
