from models import *
from config import *
from utils import *
from processing import *

def main():
   df_emp_exp_and_calls = read_csv(spark=spark_session, file_path=f"{INPUT_FILE}\dataset_one.csv",df_schema=sch_emp_exp_calls)
   df_emp_per_and_sales = read_csv(spark=spark_session, file_path=f"{INPUT_FILE}\dataset_two.csv",df_schema=sch_emp_per_sales)
   process_output_one(df_emp_exp_and_calls, df_emp_per_and_sales, f"{OUTPUT_FILE}/it_data")
if __name__ == "__main__":
    main()