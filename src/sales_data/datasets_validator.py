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

    def _validate_record(self, row: dict) -> Optional[dict]:
        """Validate a single row using Pydantic"""
        try:
            validated = self.PydanticModel(**row)
            return validated.model_dump()
        except (ValueError, AttributeError) as e:
            logger.error(f"Validation Error: {e} | Row: {row} ")
            return None

    def df_validate(self, spark: SparkSession):
        """Create new spark dataframe dropping None rows"""
        dataframe = read_csv(spark, self.df_schema, self.dataset_path)
        validated_data = [
            self._validate_record(row.asDict()) for row in dataframe.collect()
        ]
        validated_data = [row for row in validated_data if row]

        return spark.createDataFrame(validated_data)
