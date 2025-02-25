from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from sales_data.config import logger


def read_csv(
    spark: SparkSession, df_schema: StructType, dataset_path: str
) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.
    Args:
        spark (SparkSession): The Spark session.
        input_path (str): The path to the CSV file.
    Returns:
        pyspark.sql.DataFrame: The loaded DataFrame.
    """
    return spark.read.schema(df_schema).option("header", "true").csv(dataset_path)


def write_csv(df: DataFrame, output_name: str, output_dir: str) -> None:
    """
    Write a DataFrame to a CSV file in the specified directory.
    Args:
        df (DataFrame): The DataFrame to be written.
        output_dir (str): The output directory where the file will be saved.
    """
    logger.info(f"Writing Output {output_name} to location: data/output/{output_name}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
