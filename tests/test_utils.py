import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from unittest.mock import patch
import chispa
from sales_data.utils import ReadAndValidateCsvData
from sales_data.models import EmployeeExpertiseAndCallsInfo


@pytest.fixture(scope="session")
def spark():
    """Fixture to initialize a Spark session"""
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()


@pytest.fixture
def df_schema():
    """Fixture to define schema for the DataFrame"""
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", IntegerType(), True),
            StructField("calls_successful", IntegerType(), True),
        ]
    )


@pytest.fixture
def PydanticModel():
    """Fixture to define a Pydantic model for validation"""
    return EmployeeExpertiseAndCallsInfo


@pytest.fixture
def input_path():
    """Fixture for the input path of the CSV"""
    return "/path/to/fake/csv"  # Can be mocked during the test


class TestUtils:
    def test_validate_record_invalid_data(self, df_schema, PydanticModel):
        """Test that a row with wrong data in _validate_record returns None and raises an exception"""
        read_and_validate = ReadAndValidateCsvData(df_schema, PydanticModel, input_path)

        invalid_row = {
            "id": 1,
            "area": "IT",
            "calls_made": "Not Calls",
            "calls_successful": 41,
        }
        result = read_and_validate._validate_record(invalid_row)
        assert result is None

    def test_validated_df(self, spark, df_schema, PydanticModel, input_path):
        """Test that the validated DataFrame filters out rows with None values"""
        valid_data = [
            {
                "id": 1,
                "area": "IT",
                "calls_made": 54,
                "calls_successful": 41,
            },
            {
                "id": 4,
                "area": "Games",
                "calls_made": 84,
                "calls_successful": 61,
            },
        ]
        invalid_data = [
            {
                "id": None,
                "area": "Finance",
                "calls_made": 5,
                "calls_successful": 41,
            },
            {
                "id": 5,
                "area": None,
                "calls_made": 84,
                "calls_successful": 61,
            },
        ]
        # Create a DataFrame from the valid and invalid data
        input_data = valid_data + invalid_data
        df = spark.createDataFrame(input_data, df_schema)

        # Mock _read_csv to return the test DataFrame
        read_and_validate = ReadAndValidateCsvData(df_schema, PydanticModel, input_path)
        with patch.object(read_and_validate, "_read_csv", return_value=df):
            validated_df = read_and_validate.validated_df(spark)
            # Spark changes IntegerType() types to LongType() so need to tranform back
            validated_df = validated_df.select(
                col("id").cast("int"),
                col("area"),
                col("calls_made").cast("int"),
                col("calls_successful").cast("int"),
            )

        # Expected Dataframe
        df_expected = spark.createDataFrame(valid_data, df_schema)

        # Use Chispa to check if the DataFrame content is as expected
        chispa.assert_df_equality(validated_df, df_expected)
