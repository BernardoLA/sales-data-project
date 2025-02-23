from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, regexp_extract, sum, col, round
from pyspark.sql.types import DoubleType
from sales_data.utils import write_csv


class OutputProcessor:
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
        self.df2 = self.df2.withColumnRenamed("id", "emp_id")
        df_joined = self.df1.join(
            self.df2, self.df1["id"] == self.df2["emp_id"], "inner"
        )
        return df_joined

    def process_it_data(self) -> None:
        df_it_data = (
            self.df.filter(self.df["area"] == "IT")
            .orderBy(desc("sales_amount"))
            .limit(100)
            .drop("emp_id")
        )
        write_csv(df_it_data, self.outputpath1)

    def process_marketing_address_info(self) -> None:
        """
        Extracts zip code from the address column.

        :param df: Input PySpark DataFrame
        :param output_path_mark_add_info: output path of resulting df
        :return: csv with address and zip_code column
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

    def run_all_outputs(self):
        self.process_it_data()
        self.process_marketing_address_info()
        self.process_department_breakdown()
