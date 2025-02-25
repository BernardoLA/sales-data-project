from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc, regexp_extract, sum, col, round, length, rank
from pyspark.sql.types import DoubleType
from sales_data.utils import write_csv
from sales_data.config import logger


class OutputProcessor:
    """
    Processes sales data by joining DataFrames and generating reports.

    :param df_expertise_calls: First Dataframe containing employee sales data.
    :type df_expertise_calls: DataFrame
    :param df2: Second DataFrame containing employee info.
    :type df2: DataFrame
    :param output_path_dataset_one: Output path for employees selling IT products.
    :type output_path_dataset_one: str
    :param output_path_dataset_two: Output path for employees selling Marketing products.
    :type output_path_dataset_two: str
    :param output_path_dataset_three: Output path for department breakdown report.
    :type output_path_dataset_three: str
    :param df3: Optional third DataFrame, defaults to None.
    :type df3: DataFrame, optional
    """

    def __init__(
        self,
        df_expertise_calls: DataFrame,
        df_personal_sales: DataFrame,
        output_path_dataset_one: str,
        output_path_dataset_two: str,
        output_path_dataset_three: str,
        output_path_dataset_four: str,
        df3: DataFrame = None,
    ):
        self.df_expertise_calls = df_expertise_calls
        self.df_personal_sales = df_personal_sales
        self.output_path_dataset_one = output_path_dataset_one
        self.output_path_dataset_two = output_path_dataset_two
        self.output_path_dataset_three = output_path_dataset_three
        self.output_path_dataset_four = output_path_dataset_four
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
        - Write CSV to `self.output_path_dataset_one`.

        :return: None
        """
        logger.info("Processing Output it data...")
        df_it_data = (
            self.df_employee_info.filter(self.df_employee_info["area"] == "IT")
            .orderBy(desc("sales_amount"))
            .limit(100)
            .drop("emp_id")
        )
        min_top100_sales = (
            df_it_data.select("sales_amount")
            .agg({"sales_amount": "min"})
            .collect()[0][0]
        )

        if min_top100_sales <= 0:
            logger.critical(
                "Stopping the application. Zero sales are not expected. The data in dataset two needs to be checked."
            )
            raise Exception

        logger.info("Successfully processed top 100 employees selling it products.")
        write_csv(df_it_data, "it_data", self.output_path_dataset_one)

    def process_marketing_address_info(self) -> None:
        """
        Extracts the zip code from the address column for Marketing employees and saves the result.

        - Filter `self.df_employee_info` by Marketing employees.
        - Create zip code column with regex expression.
        - Write CSV to `self.outputpath2`.

        :return: None
        """
        logger.info("Processing Output marketing address info...")

        zip_code_pattern = r"(\d{4} [A-Z]{2})"
        df_marketing_address = self.df_employee_info.filter(
            self.df_employee_info["area"] == "Marketing"
        )
        df_marketing_address = df_marketing_address.withColumn(
            "zip_code",
            regexp_extract(df_marketing_address["address"], zip_code_pattern, 1),
        )
        df_marketing_address = df_marketing_address.select("address", "zip_code")

        if df_marketing_address.filter(length(col("zip_code")) != 7).count() != 0:
            logger.warning(
                "There are non Dutch zip codes.\nCheck logs for incorrect addresses."
            )

        else:
            logger.info(
                "Sucessfully processed addresses and zip codes of employees selling marketing products."
            )
        write_csv(
            df_marketing_address, "marketing_address_info", self.output_path_dataset_two
        )

    def process_department_breakdown(self) -> None:
        """
        Aggregates sales and calculates success rate per department.

        - Groups the data by department.
        - Computes total sales per department.
        - Calculates the success rate of calls as a percentage.
        - Sorts results by sales amount and success rate.
        - Saves the processed data to CSV.
        - Writes CSV to `self.output_path_dataset_three`

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
        if df_dpt_breakdown.filter(col("success_rate (%)") > 100).count() > 0:
            logger.critical(
                "Stopping the application. Success Rate above 100 percent is not possible.\nThe data in datasets one and two needs to be checked."
            )
            raise Exception
        logger.info(
            "Sucessfully processed sales and calls success rate per department."
        )
        write_csv(
            df_dpt_breakdown, "department_breakdown", self.output_path_dataset_three
        )

    def process_top_three_per_dpt(self) -> None:
        """
        Selects the top 3 employees per department based on successful call rates.

        - Calculates the rate of successful calls per employee.
        - Ranks employees within each department by their success rate.
        - Brings back sales-related columns.
        - Filters for top 3 employees per department.
        - Saves the results.

        :return: None
        """
        logger.info("Processing top 3 employees per department...")

        # Step 1: Calculate the rate of successful calls per employee
        df_employee_rate = self.df_employee_info.withColumn(
            "rate_successful_calls (%)",
            (col("calls_successful") / col("calls_made") * 100),
        )

        # Step 2: Define a partition by department and rank employees within each department
        window_spec = Window.partitionBy("area").orderBy(
            col("rate_successful_calls (%)").desc()
        )

        df_ranked = df_employee_rate.withColumn("rank", rank().over(window_spec))

        # Step 3: Select relevant columns (including sales data)
        df_selected = df_ranked.select(
            "id",
            "area",
            "rate_successful_calls (%)",
            "sales_amount",
            "rank",
        )

        # Step 4: Filter for employees ranked in the top 3 per department
        df_top_three = df_selected.filter(col("rank") <= 3)

        # Save results
        write_csv(
            df_top_three, "top_three_per_department", self.output_path_dataset_four
        )

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
