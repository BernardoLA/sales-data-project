from pyspark.sql import SparkSession
from pathlib import Path

# Spark Session
spark_session = SparkSession.builder.master("local[*]").appName("ABN AMRO Programming Exercise").getOrCreate()

# global path variables
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "../../data"
INPUT_FILE = DATA_DIR / "input/"
OUTPUT_FILE = DATA_DIR / "output/"
