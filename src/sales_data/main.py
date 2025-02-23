from sales_data import INPUT_FILE, OUTPUT_FILE, logger, spark
from sales_data.utils import ReadAndValidateCsvData
from sales_data.output_processor import OutputProcessor
from sales_data.models import (
    sch_emp_exp_calls,
    sch_emp_per_sales,
    EmployeeExpertiseAndCallsInfo,
    EmployePersonalAndSalesInfo,
)


def main():
    logger.info("Starting Application...")
    ## Read all csv files and validate records
    # Validate with Pydantic lines with bad input

    employee_expertise_data = ReadAndValidateCsvData(
        sch_emp_exp_calls,
        EmployeeExpertiseAndCallsInfo,
        f"{INPUT_FILE}\dataset_one.csv",
    )
    employee_personal_data = ReadAndValidateCsvData(
        sch_emp_per_sales, EmployePersonalAndSalesInfo, f"{INPUT_FILE}\dataset_two.csv"
    )
    # Store df with employee expertise data (dataset_one)
    df_expertise_validated_data = employee_expertise_data.validated_df(spark)

    # Store df with employee expertise data (dataset_two)
    df_personal_validated_data = employee_personal_data.validated_df(spark)

    ## Process the outputs
    process_outputs = OutputProcessor(
        df_expertise_validated_data,
        df_personal_validated_data,
        f"{OUTPUT_FILE}/it_data",
        f"{OUTPUT_FILE}/marketing_address_info",
        f"{OUTPUT_FILE}/department_breakdown",
    )
    # process_outputs.process_it_data(f"{OUTPUT_FILE}/it_data")
    process_outputs.run_all_outputs()
    logger.info("Closing Application...")


if __name__ == "__main__":
    main()
