from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, regexp_extract, sum, col, round
from pyspark.sql.types import DoubleType
from sales_data.utils import write_csv
from sales_data.config import logger


class OutputProcessor:
    """
    Processes sales data by joining DataFrames and generating reports.

    :param df_expertise_calls: Firstdf_personal_sales containing employee sales data.
    :type df_expertise_calls: DataFdf_personal_sales
    :param df2: Second DataFrame containing employee info.
    :type df2: DataFrame
    :param output_path1: Output path for IT department data.
    :type output_path1: str
    :param output_path2: Output path for Marketing department address info.
    :type output_path2: str
    :param output_path3: Output path for department breakdown report.
    :type output_path3: str
    :param df3: Optional third DataFrame, defaults to None.
    :type df3: DataFrame, optional
    """

    def __init__(
        self,
        df_expertise_calls: DataFrame,
        df_personal_sales: DataFrame,
        input_path_dataset_one: str,
        input_path_dataset_two: str,
        input_path_dataset_three: str = None,
        df3: DataFrame = None,
    ):
        self.df_expertise_calls = df_expertise_calls
        self.df_personal_sales = df_personal_sales
        self.input_path_dataset_one = input_path_dataset_one
        self.input_path_dataset_two = input_path_dataset_two
        self.df3 = df3
        self.outputpath3 = input_path_dataset_three
        self.df_employee_info = self._join_df_expertise_df_personal()

    def _join_df_expertise_df_personal(self) -> DataFrame:
        """
        Joins `df_expertise_calls` and `df_personal_sales` on the employee ID column.

        - Rename `self.df_personal_sales` id to `emp_id`
        - Joins `self.df_expertise_calls` id on `self.df_personal_sales` emp_id

        :return: Joined DataFrame containing employee personal and sales info.
        :rtype: DataFrame
        """
        self.df_personal_sales = self.df_personal_sales.withColumnRenamed(
            "id", "emp_id"
        )
        df_employee_info = self.df_expertise_calls.join(
            self.df_personal_sales,
            self.df_expertise_calls["id"] == self.df_personal_sales["emp_id"],
            "inner",
        )
        return df_employee_info

    def process_it_data(self) -> None:
        """
        Filters employees selling IT products, sorts by sales amount, and saves the top 100 records.

        - Filters `self.df_employee_info` by employees selling IT products.
        - Selects top 100 employees based on sales amount (descending order).
        - Write CSV to `self.outputpath1`.

        :return: None
        """
        logger.info("Processing output it data...")
        df_it_data = (
            self.df_employee_info.filter(self.df_employee_info["area"] == "IT")
            .orderBy(desc("sales_amount"))
            .limit(100)
            .drop("emp_id")
        )
        if df_it_data.count() == 100:
            logger.info("Successfully processed top 100 agents selling IT products.")
        else:
            logger.warning(
                f"There are {df_it_data.count} records. Check your logs and csv files."
            )
        write_csv(df_it_data, "it_data", self.input_path_dataset_one)

    def process_marketing_address_info(self) -> None:
        """
        Extracts the zip code from the address column for Marketing employees and saves the result.

        - Filter `self.df_employee_info` by Marketing employees.
        - Create zip code column with regex expression.
        - Write CSV to `self.outputpath2`.

        :return: None
        """
        logger.info("Processing output marketing address info...")

        zip_code_pattern = r"(\d{4} [A-Z]{2})"
        df_marketing_address = self.df_employee_info.filter(
            self.df_employee_info["area"] == "Marketing"
        )
        df_marketing_address = df_marketing_address.withColumn(
            "zip_code",
            regexp_extract(df_marketing_address["address"], zip_code_pattern, 1),
        )
        df_marketing_address = df_marketing_address.select("address", "zip_code")

        write_csv(
            df_marketing_address, "marketing_address_info", self.input_path_dataset_two
        )

    def process_department_breakdown(self) -> None:
        """
        Aggregates sales and calculates success rate per department.

        - Groups the data by department.
        - Computes total sales per department.
        - Calculates the success rate of calls as a percentage.
        - Sorts results by sales amount and success rate.
        - Saves the processed data to CSV.
        - Writes CSV to `self.outputpath3`

        :return: None
        """
        logger.info("Processing department breakdown...")
        df_dpt_breakdown = (
            self.df_employee_info.groupBy(self.df_employee_info["area"])
            .agg(
                sum("sales_amount").alias("sales_amount"),
                (sum("calls_successful") / sum("calls_made") * 100).alias(
                    "success_rate (%)"
                ),
            )
            .withColumn(
                "sales_amount", round(col("sales_amount"), 2).cast(DoubleType())
            )
            .withColumn(
                "success_rate (%)", round(col("success_rate (%)"), 2).cast(DoubleType())
            )
            .orderBy(["sales_amount", "success_rate (%)"], ascending=[False, False])
        )
        write_csv(df_dpt_breakdown, "department_breakdown", self.outputpath3)

    def run_all_outputs(self) -> None:
        """
        Executes all processing functions in sequence.

        - Processes IT department sales data.
        - Extracts and processes Marketing department address information.
        - Computes department-wise sales breakdown.
        - Saves the results to their respective output paths.

        :return: None
        """
        self.process_it_data()
        self.process_marketing_address_info()
        self.process_department_breakdown()
