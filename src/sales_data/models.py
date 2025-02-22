from pydantic import BaseModel, Field
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

    id: int = Field(examples=[1, 100], description="Employee's id", gt=0)
    area: str = Field(examples=["Marketing"], description="Employee's are of expertise")
    calls_made: int = Field(
        examples=[54], description="Number of calls made by an employee", ge=0
    )
    calls_successful: int = Field(
        examples=[30], description="Number of successful call made by an employee", ge=0
    )


class EmployePersonalAndSalesInfo(BaseModel):
    """Pydantic model for validating employee personal and sales records."""

    id: int = Field(examples=[1], description="Employee's id", gt=0)
    name: str = Field(examples=["Berat Hakker"], description="Employee's name")
    address: str = Field(
        examples=["Pienlaan 5, 7356 GF, Kerkdriel", "3255 WJ, Hillegom"],
        description="Employee's address",
    )
    sales_amount: float = Field(
        examples=[928.28], description="Sales made by an employee", ge=0
    )


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
