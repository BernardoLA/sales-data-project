from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pydantic import BaseModel
from typing import Optional


class ReadAndValidateCsvData:
    def __init__(
        self,
        df_schema: StructType,
        PydanticModel: BaseModel,
        input_path: str,
    ):
        self.df_schema = df_schema
        self.PydanticModel = PydanticModel
        self.input_path = input_path

    def _read_csv(self, spark: DataFrame) -> DataFrame:
        """
        Read a CSV file into a Spark DataFrame.
        Args:
            spark (SparkSession): The Spark session.
            input_path (str): The path to the CSV file.
        Returns:
            pyspark.sql.DataFrame: The loaded DataFrame.
        """
        return (
            spark.read.schema(self.df_schema)
            .option("header", "true")
            .csv(self.input_path)
        )

    def _validate_record(self, record: dict) -> Optional[dict]:
        """Validate a single record using Pydantic"""
        try:
            validated = self.PydanticModel(**record)
            return validated.model_dump()  # Convert back to dictionary if valid
        except ValueError as e:
            print(f"Validation Error: {e} | Record: {validated} ")
            return None

    def validated_df(self, spark: SparkSession):
        """Create new spark dataframe dropping None rows"""
        dataframe = self._read_csv(spark)
        validated_data = [
            self._validate_record(record)
            for record in dataframe.toPandas().to_dict(orient="records")
        ]
        validated_data = [row for row in validated_data if row]

        return spark.createDataFrame(validated_data)


def write_csv(df: DataFrame, output_dir: str) -> None:
    """
    Write a DataFrame to a CSV file in the specified directory.
    Args:
        df (DataFrame): The DataFrame to be written.
        output_dir (str): The output directory where the file will be saved.
    """
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
