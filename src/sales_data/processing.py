from pyspark.sql.functions import desc
from sales_data.utils import write_csv, DataFrame


def process_output_one(
    df_emp_exp_and_calls: DataFrame, df_emp_per_and_sales: DataFrame, df_output_path
) -> None:

    # rename id before join to avoid COLUMN_ALREADY_EXISTS error
    df_emp_per_and_sales = df_emp_per_and_sales.withColumnRenamed("id", "emp_id")

    # Join the datasets, filter for IT department and sort desc top 100
    df_final = (
        df_emp_exp_and_calls.join(
            df_emp_per_and_sales,
            (df_emp_exp_and_calls["area"] == "IT")
            & (df_emp_exp_and_calls["id"] == df_emp_per_and_sales["emp_id"]),
            "inner",
        )
        .drop("emp_id")
        .orderBy(desc("sales_amount"))
        .limit(100)
    )
    # Write to CSV
    write_csv(df_final, df_output_path)
