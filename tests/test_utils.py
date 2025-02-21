# from sales_data.utils import read_csv, write_csv

# def test_read_csv(spark):
#     df = read_csv(spark, "tests/test_data/sample.csv")
#     assert df.count() > 0  # Ensure data was loaded

# def test_write_csv(spark, tmp_path):
#     df = ...  # Create small PySpark DataFrame
#     output_path = tmp_path / "output"

#     write_csv(df, str(output_path))

#     assert (output_path / "part-00000.csv").exists()  # Check if file was written
