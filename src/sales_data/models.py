from pydantic import BaseModel
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)


# Pydantic Models for validation
class EmployeeExpertiseAndCallsInfo(BaseModel):
    """Pydantic model for validating employee sales and calls records."""

    id: int
    area: str
    calls_made: int
    calls_successful: int


class EmployePersonalAndSalesInfo(BaseModel):
    """Pydantic model for validating employee personal and sales records."""

    id: int
    name: str
    address: str
    sales_amount: float


# Datasets schemas
sch_emp_exp_calls = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("area", StringType(), False),
        StructField("calls_made", IntegerType(), True),
        StructField("calls_successful", IntegerType(), True),
    ]
)
sch_emp_per_sales = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("sales_amount", FloatType(), True),
    ]
)
