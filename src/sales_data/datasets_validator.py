from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pydantic import BaseModel
from typing import Optional
from sales_data.config import logger
from sales_data.utils import read_csv


class DatasetValidator:
    def __init__(
        self,
        df_schema: StructType,
        PydanticModel: BaseModel,
        dataset_path: str,
    ):
        self.df_schema = df_schema
        self.PydanticModel = PydanticModel
        self.dataset_path = dataset_path
        self.dataset_name = self.extract_dataset_name()

    def _validate_record(self, row: dict) -> Optional[dict]:
        """Validate a single row using Pydantic"""
        try:
            validated = self.PydanticModel(**row)
            return validated.model_dump()
        except (ValueError, AttributeError) as e:
            logger.error(f"{e} | Row: {row} ")
            return None

    def validate_df(self, spark: SparkSession):
        """Create new spark dataframe dropping None rows"""
        dataframe = read_csv(spark, self.df_schema, self.dataset_path)
        valid_and_invalid_data = [
            self._validate_record(row.asDict()) for row in dataframe.collect()
        ]
        valid_data = [row for row in valid_and_invalid_data if row]
        invalid_records = len(valid_and_invalid_data) - len(valid_data)

        if invalid_records > 0:
            logger.warning(
                f"A total of {invalid_records} invalid records excluded from dataset {self.dataset_name}"
            )
        else:
            logger.info(f"There were no invalid records found in {self.dataset_name}")

        return spark.createDataFrame(valid_data)

    def extract_dataset_name(self) -> str:
        """Extract dataset name from dataset_path"""
        dataset_csv = self.dataset_path.split("\\")[-1]
        dataset = dataset_csv.replace(".csv", "")
        return dataset
