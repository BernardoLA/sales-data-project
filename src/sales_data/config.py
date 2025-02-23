from pyspark.sql import SparkSession
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

# global path variables
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "../../data"
INPUT_FILE = DATA_DIR / "input/"
OUTPUT_FILE = DATA_DIR / "output/"


# Logging settings
LOG_DIR = Path(__file__).resolve().parent / "../logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "app.log"

# Configure Rotating Logs
logging.basicConfig(
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=1000000, backupCount=5),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Spark Session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("ABN AMRO Programming Exercise")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)
