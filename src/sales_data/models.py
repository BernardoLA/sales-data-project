from pydantic import BaseModel, Field, field_validator
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)


# Pydantic Models for validation
class EmployeeExpertiseCalls(BaseModel):
    """
    Pydantic model for validating employee expertise and calls info.

    :param id: Employee's unique identifier.
    :type id: int
    :param area: Employee's area of expertise.
    :type area: str
    :param calls_made: Number of calls made by an employee.
    :type calls_made: int
    :param calls_successful: Number of successful calls made by an employee.
    :type calls_successful: int
    """

    id: int = Field(examples=[1, 100], description="Employee's id", gt=0)
    area: str = Field(examples=["Marketing"], description="Employee's are of expertise")
    calls_made: int = Field(
        examples=[54], description="Number of calls made by an employee", ge=0
    )
    calls_successful: int = Field(
        examples=[30], description="Number of successful call made by an employee", ge=0
    )

    @field_validator("area", mode="before")
    def format_area(cls, area: str) -> str:
        """
        Formats the area field to ensure consistency.

        - Converts 'it' and 'hr' to uppercase.
        - Capitalizes other areas.

        :param area: Area of expertise provided in input.
        :type area: str
        :return: Formatted area string.
        :rtype: str
        """
        if area.lower() == "it" or area.lower() == "hr":
            return area.upper()
        else:
            return area.capitalize()


class EmployeePersonalInfo(BaseModel):
    """
    Pydantic model for validating employee personal and sales records.

    :param id: Employee's unique identifier.
    :type id: int
    :param name: Employee's full name.
    :type name: str
    :param address: Employee's address.
    :type address: str
    :param sales_amount: Total sales made by the employee.
    :type sales_amount: float
    """

    id: int = Field(examples=[1], description="Employee's id", gt=0)
    name: str = Field(examples=["Berat Hakker"], description="Employee's name")
    address: str = Field(
        examples=[
            "Pienlaan 5, 7356 GF, Kerkdriel",
            "3255 WJ, Hillegom",
            "32559 WJ, Hillegom",
            "3255WJ, Hillegom",
        ],
        description="Employee's address",
    )
    sales_amount: float = Field(
        examples=[928.28], description="Sales made by an employee", ge=0
    )


"""
Schema definition for the Employee Expertise and Calls dataset.

- `id` (int, required): Employee's unique identifier.
- `area` (str, required): Area of expertise.
- `calls_made` (int, optional): Number of calls made by the employee.
- `calls_successful` (int, optional): Number of successful calls.
"""
sch_expertise_calls = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("area", StringType(), False),
        StructField("calls_made", IntegerType(), True),
        StructField("calls_successful", IntegerType(), True),
    ]
)
"""
Schema definition for the Employee Personal and Sales dataset.

- `id` (int, required): Employee's unique identifier.
- `name` (str, required): Employee's full name.
- `address` (str, required): Employee's address.
- `sales_amount` (float, optional): Total sales made by the employee.
"""
sch_personal_sales = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), True),
        StructField("sales_amount", FloatType(), True),
    ]
)
