from pydantic import BaseModel, ValidationError
from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pytest

# import os
import sys

spark = SparkSession.builder.master("local[*]").appName("AbnaProjectApp").getOrCreate()


class Product(BaseModel):
    name: str
    price: float
    quantity: int

# import os 
# os.environ["PYSPARK_PYTHON"] = "python"

def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")

try:
    invalid_data = {
        "name": "Laptop",
        "price": "127.56",  # This should be a float, not a string
        "quantity": 5
    }

    product = Product(**invalid_data)
    print(product)
except ValidationError as e:
    print(e.json())


def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "**li**"),
        ("#::luisa", "luisa")
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
        .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    assert_column_equality(df, "clean_name", "expected_name")


test_remove_non_word_characters_short()


"""
This test code below worked!
"""
# # Initialize SparkSession
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# spark = SparkSession.builder.getOrCreate()
# print(sys.executable)

# # Create a sample DataFrame
# data = [("Alice", 28), ("Bob", 35), ("Chsrlie", 22)]
# columns = ["name", "age"]
# df = spark.createDataFrame(data, columns)

# # Perform a simple transformation (add 1 to each person's age)
# df_transformed = df.withColumn("age_plus_one", F.col("age") + 1)

# # Show the resulting DataFrame
# df_transformed.show()