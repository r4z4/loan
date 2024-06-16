from setuptools import find_packages, setup

setup(
    name="loan",
    packages=find_packages(exclude=["loan_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "duckdb",
        "pandas",
        "sqlescapy",
        "lxml",
        "html5lib"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "localstack", "awscli", "awscli-local"]},
)
