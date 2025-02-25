from sales_data.config import OUTPUT_FILE, logger, spark
from sales_data.dataset_validator import DatasetValidator
from sales_data.output_processor import OutputProcessor
from sales_data.models import (
    sch_expertise_calls,
    sch_personal_sales,
    EmployeeExpertiseCalls,
    EmployeePersonalInfo,
)
import click


def run_etl(
    input_path_dataset_one: str,
    input_path_dataset_two: str,
    input_path_dataset_three: str = None,
):
    ## Read all csv files and validate records
    # Validate with Pydantic lines with bad input

    employee_expertise_calls = DatasetValidator(
        sch_expertise_calls,
        EmployeeExpertiseCalls,
        input_path_dataset_one,
    )
    employee_personal_sales = DatasetValidator(
        sch_personal_sales, EmployeePersonalInfo, input_path_dataset_two
    )

    logger.info(f"Validating {employee_expertise_calls.dataset_name} with Pydantic.")
    df_expertise_calls_validated = employee_expertise_calls.validate_df(spark)

    logger.info(f"Validating {employee_personal_sales.dataset_name} with Pydantic.")
    df_personal_sales_validated = employee_personal_sales.validate_df(spark)

    logger.info("Processing Outputs sequentially...")

    # Create Output Processor instance
    process_outputs = OutputProcessor(
        df_expertise_calls_validated,
        df_personal_sales_validated,
        f"{OUTPUT_FILE}/it_data",
        f"{OUTPUT_FILE}/marketing_address_info",
        f"{OUTPUT_FILE}/department_breakdown",
        f"{OUTPUT_FILE}/top_3",
    )

    process_outputs.run_all_outputs()
    logger.info("Closing Application...")


@click.command()
@click.argument("dataset_one_path")
@click.argument("dataset_two_path")
def sales_data(dataset_one_path: str, dataset_two_path: str):
    click.echo(
        f"Starting pipelines for datasets: \n - {dataset_one_path}\n - {dataset_two_path}"
    )
    run_etl(dataset_one_path, dataset_two_path)


if __name__ == "__main__":
    sales_data()
