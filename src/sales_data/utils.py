from pyspark.sql import SparkSession, DataFrame
from pydantic import BaseModel
from pyspark.sql.types import StructType


def read_csv(spark: SparkSession, file_path: str, df_schema: StructType) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.
    Args:
        spark (SparkSession): The Spark session.
        file_path (str): The path to the CSV file.
    Returns:
        pyspark.sql.DataFrame: The loaded DataFrame.
    """
    return spark.read.schema(df_schema).option("header", "true").csv(file_path)


def write_csv(df: DataFrame, output_dir: str) -> None:
    """
    Write a DataFrame to a CSV file in the specified directory.
    Args:
        df (DataFrame): The DataFrame to be written.
        output_dir (str): The output directory where the file will be saved.
    """
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
