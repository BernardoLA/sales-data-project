from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, regexp_extract, sum, col, round
from pyspark.sql.types import DoubleType
from sales_data.utils import write_csv


class OutputProcessor:
    """
    Processes sales data by joining DataFrames and generating reports.

    :param df1: First DataFrame containing employee sales data.
    :type df1: DataFrame
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
        df1: DataFrame,
        df2: DataFrame,
        output_path1: str,
        output_path2: str,
        output_path3: str,
        df3: DataFrame = None,
    ):
        self.df1 = df1
        self.df2 = df2
        self.df3 = df3
        self.outputpath1 = output_path1
        self.outputpath2 = output_path2
        self.outputpath3 = output_path3
        self.df = self._join_df1_df2()

    def _join_df1_df2(self) -> DataFrame:
        """
        Joins `df1` and `df2` on the employee ID column.

        - Rename `self.df2` id to `emp_id`
        - Joins `self.df1` id on `self.df2` emp_id

        :return: Joined DataFrame containing employee personal and sales info.
        :rtype: DataFrame
        """        
        self.df2 = self.df2.withColumnRenamed("id", "emp_id")
        df_joined = self.df1.join(
            self.df2, self.df1["id"] == self.df2["emp_id"], "inner"
        )
        return df_joined

    def process_it_data(self) -> None:
        """
        Filters IT department employees, sorts by sales amount, and saves the top 100 records.

        - Filters `self.df` by IT department employees.
        - Selects top 100 employees based on sales amount (descending order).
        - Write CSV to `self.outputpath1`.
        
        :return: None
        """        
        df_it_data = (
            self.df.filter(self.df["area"] == "IT")
            .orderBy(desc("sales_amount"))
            .limit(100)
            .drop("emp_id")
        )
        write_csv(df_it_data, self.outputpath1)

    def process_marketing_address_info(self) -> None:
        """
        Extracts the zip code from the address column for Marketing employees and saves the result.

        - Filter `self.df` by Marketing employees.
        - Create zip code column with regex expression.
        - Write CSV to `self.outputpath2`.

        :return: None
        """        
        zip_code_pattern = r"(\d{4} [A-Z]{2})"
        df_marketing_address = self.df.filter(self.df["area"] == "Marketing")
        df_marketing_address = df_marketing_address.withColumn(
            "zip_code",
            regexp_extract(df_marketing_address["address"], zip_code_pattern, 1),
        )
        df_marketing_address = df_marketing_address.select("address", "zip_code")
        write_csv(df_marketing_address, self.outputpath2)

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
        df_dpt_breakdown = (
            self.df.groupBy(self.df["area"])
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
        df_dpt_breakdown.show()
        write_csv(df_dpt_breakdown, self.outputpath3)

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
