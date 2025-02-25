from setuptools import setup, find_packages

setup(
    name="abn_sales_data_pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "click==8.1.8",
        "pydantic==2.10.6",
        "pyspark==3.5.0",
        "chispa==0.10.1",
    ],
    extras_require={
        "dev": [
            "pytest==8.3.4",
            "black==25.1.0",
            "ruff==0.9.7",
            "mypy-extensions==1.0.0",
        ]
    },
    entry_points={"console_scripts": ["sales-data=sales_data.main:sales_data"]},
    python_requires="==3.10.10",
    description="A pipeline for processing assignment sales data.",
    author="Bernardo Leivas",
    author_email="bernardoleivas@gmail.com",
    url="https://github.com/BernardoLA/sales-data-project",
)
