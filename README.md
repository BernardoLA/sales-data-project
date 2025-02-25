# Product Name

This project is part of a data engineering assignment and involves processing 3 CSV files and extracting insights from the data. The main goal of the solution is to leverage best practices such as unit testing, data validation (Pydantic), logging, packaging, CI automation and clean code.


## Installation

### Prerequisites
* Python 3.10
* Pyspark (Check online how to install Spark locally)

### Steps
* Clone the repository:
* ```
  git clone https://github.com/BernardoLA/sales-data-project
  cd <project directory>
  ```
* Create a virtual environment
  ```
  python -m venv venv
  src\Scripts\activate # windows
  source venv/bin/activate  # macOS/Linux
  ```
* Install dependencies of the project
  ```
  pip install -r requirements.txt
  ```

### Usage
To run the pipelines and process all outputs it's needed to provide the paths to the two datasets:
```
python sales_data/main.py "path/to/dataset_one.csv" "path/to/dataset_two.csv"
```
Alternatively, it's possible to install this project as a package from the root directory and run it by its entry point:
```
pip install -e .
sales-data "path/to/dataset_one.csv" "path/to/dataset_two.csv"
```
