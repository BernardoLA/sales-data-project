from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, regexp_extract, col, lower
from sales_data.utils import write_csv


class ProcessOutputs:
    def __init__(self, df1: DataFrame, df2: DataFrame, df3: DataFrame = None):
        self.df1 = df1
        self.df2 = df2
        self.df3 = df3

    def join_df1_df2(self) -> DataFrame:
        self.df2 = self.df2.withColumnRenamed("id", "emp_id")
        df_joined = self.df1.join(
            self.df2, self.df1["id"] == self.df2["emp_id"], "inner"
        )
        return df_joined

    def process_it_data(self, df: DataFrame, output_path_it_data: str) -> None:
        df = (
            df.filter(lower(df["area"]) == "it")
            .orderBy(desc("sales_amount"))
            .limit(100)
            .drop("emp_id")
        )
        write_csv(df, output_path_it_data)

    def process_marketing_address_info(
        self, df: DataFrame, output_path_mark_add_info: str
    ) -> None:
        """
        Extracts zip code from the address column.

        :param df: Input PySpark DataFrame
        :param output_path_mark_add_info: output path of resulting df
        :return: csv with an additional 'zip_code' column
        """
        zip_code_pattern = r"(\d{4} [A-Z]{2})"
        df = df.filter(lower(df["area"]) == "marketing")
        df = df.withColumn(
            "zip_code", regexp_extract(df["address"], zip_code_pattern, 1)
        )
        df = df.select("address", "zip_code")
        write_csv(df, output_path_mark_add_info)

    def run_all_outputs(self, output_path_it_data: str, output_path_mark_add_info: str):
        df = self.join_df1_df2()
        self.process_it_data(df, output_path_it_data)
        self.process_marketing_address_info(df, output_path_mark_add_info)
