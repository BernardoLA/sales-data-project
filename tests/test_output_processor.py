from chispa.dataframe_comparer import assert_df_equality, assert_approx_df_equality
from unittest.mock import patch
from sales_data.output_processor import OutputProcessor
import pytest
from pyspark.sql import SparkSession, DataFrame


@pytest.fixture(scope="module")
def spark():
    # Set up a Spark session for testing
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture(scope="module")
def dataset_one(spark: SparkSession):
    """Create common test DataFrame for reuse across tests"""
    df_data = [
        (1, "IT", 50, 30),
        (2, "IT", 40, 20),
        (3, "Marketing", 60, 40),
        (4, "Marketing", 100, 35),
        (5, "Sales", 20, 10),
        (6, "Finance", 40, 15),
        (7, "Games", 60, 30),
        (8, "Games", 70, 30),
        (9, "Games", 100, 60),
        (10, "HR", 85, 30),
        (11, "HR", 75, 40),
        (12, "Finance", 40, 15),
    ]
    df_columns = [
        "id",
        "area",
        "calls_made",
        "calls_successful",
    ]
    return spark.createDataFrame(df_data, df_columns)


@pytest.fixture(scope="module")
def dataset_two(spark: SparkSession):
    """Create common test DataFrame for reuse across tests"""
    df_data = [
        (1, "Alice", "2588 VD, Kropswolde", 5000.0),
        (2, "Bob", "1808 KR, Benningbroek", 4500.0),
        (3, "Charlie", "Thijmenweg 38, 7801 OC, Grijpskerk", 6000.0),
        (4, "Eva", "Jetlaan 816, 8779 EM, Holwierde", 3450.00),
        (5, "John", "Janlaan 4, 3319 GW, Rijs", 7000.0),
        (6, "Vincent", "Nadiaring 992, 6189 BM, Slagharen", 1500.00),
        (7, "Jolie", "Dinaweg 6, 7345 OG, Niehove", 3000.00),
        (8, "Noah", "Jorisboulevard 464, 2259 AN, Parrega", 3000.00),
        (9, "Dani", "6381 FW, Sint Anthonis", 6000.00),
        (10, "Charlote", "Bartsingel 9, 3054 TD, Westerwijtwerd", 2700.00),
        (11, "Esmee", "2926 OQ, Vlijmen", 4000.00),
        (12, "Maria", "Jorisboulevard 464, 2259 AN, Parrega", 2100.00),
    ]
    df_columns = [
        "id",
        "name",
        "address",
        "sales_amount",
    ]
    return spark.createDataFrame(df_data, df_columns)


class TestOutputProcessor:
    def test_process_it_data(
        self, spark: SparkSession, dataset_one: DataFrame, dataset_two: DataFrame
    ):
        expected_data = [
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
        df_expected = spark.createDataFrame(expected_data, expected_columns)

        # Mock write_csv to capture the DataFrame passed to it
        with patch("sales_data.output_processor.write_csv") as mock_write:
            process_outputs = OutputProcessor(
                dataset_one,
                dataset_two,
                "dummy pathone",
                "dummy pathtwo",
                "dummy paththree",
                "dummy pathfour",
            )
            process_outputs.process_it_data()
            # Ensure write_csv was called once and check the arguments passed
            mock_write.assert_called_once()

            # Get the actual DataFrame passed to write_csv
            df_actual = mock_write.call_args[0][0]

            # Compare actual vs expected DataFrame
            assert_df_equality(
                df_actual, df_expected, ignore_nullable=True, ignore_column_order=True
            )

    def test_process_marketing_address_info(
        self, spark: SparkSession, dataset_one: DataFrame, dataset_two: DataFrame
    ):
        df_data_expected = [
            ("Jetlaan 816, 8779 EM, Holwierde", "8779 EM"),
            ("Thijmenweg 38, 7801 OC, Grijpskerk", "7801 OC"),
        ]
        df_columns_expected = ["address", "zip_code"]
        df_expected = spark.createDataFrame(df_data_expected, df_columns_expected)
        print(df_expected)

        # Mock write_csv to capture the DataFrame passed to it
        with patch("sales_data.output_processor.write_csv") as mock_write:
            process_outputs = OutputProcessor(
                dataset_one,
                dataset_two,
                "dummy pathone",
                "dummy pathtwo",
                "dummy paththree",
                "dummy pathfour",
            )
            process_outputs.process_marketing_address_info()
            # Ensure write_csv was called once and check the arguments passed
            mock_write.assert_called_once()

            # Get the actual DataFrame passed to write_csv
            df_actual = mock_write.call_args[0][0]
            print(df_actual)

            # Compare actual vs expected DataFrame
            assert_df_equality(
                df_actual, df_expected, ignore_row_order=True, ignore_column_order=True
            )

    def test_process_department_breakdown_departments(
        self, spark: SparkSession, dataset_one: DataFrame, dataset_two: DataFrame
    ):
        expected_data = [
            ("Games",),
            ("IT",),
            ("Marketing",),
            ("HR",),
            ("Sales",),
            ("Finance",),
        ]

        expected_columns = ["area"]

        df_expected = spark.createDataFrame(expected_data, expected_columns)

        # Mock write_csv to capture the DataFrame passed to it
        with patch("sales_data.output_processor.write_csv") as mock_write:
            process_outputs = OutputProcessor(
                dataset_one,
                dataset_two,
                "dummy pathone",
                "dummy pathtwo",
                "dummy paththree",
                "dummy pathtfour",
            )
            process_outputs.process_department_breakdown()
            # Ensure write_csv was called once and check the arguments passed
            mock_write.assert_called_once()

            # Get the actual DataFrame passed to write_csv
            df_actual = mock_write.call_args[0][0]

            # select area column
            df_actual_area = df_actual.select("area")

            # Compare actual vs expected DataFrame
            assert_df_equality(df_actual_area, df_expected, ignore_row_order=True)

    def test_process_department_breakdown_sales_and_success_rate(
        self, spark: SparkSession, dataset_one: DataFrame, dataset_two: DataFrame
    ):
        expected_data = [
            ("Games", 12000.00, 52.00),
            ("IT", 9500.00, 55.00),
            ("Marketing", 9450.00, 46.00),
            ("HR", 6700.00, 43.00),
            ("Sales", 7000.00, 50.00),
            ("Finance", 3600.00, 37.50),
        ]

        expected_columns = [
            "area",
            "sales_amount",
            "success_rate (%)",
        ]

        df_expected = spark.createDataFrame(expected_data, expected_columns)
        # Mock write_csv to capture the DataFrame passed to it
        with patch("sales_data.output_processor.write_csv") as mock_write:
            process_outputs = OutputProcessor(
                dataset_one,
                dataset_two,
                "dummy pathone",
                "dummy pathtwo",
                "dummy paththree",
                "dummy pathfour",
            )
            process_outputs.process_department_breakdown()
            # Ensure write_csv was called once and check the arguments passed
            mock_write.assert_called_once()

            # Get the actual DataFrame passed to write_csv
            df_actual = mock_write.call_args[0][0]

            # Compare actual vs expected DataFrame
            assert_approx_df_equality(
                df_actual,
                df_expected,
                precision=0.89,
                ignore_row_order=True,
                ignore_column_order=True,
            )
