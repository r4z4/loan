from dagster import load_assets_from_package_module, repository, with_resources
from loan import assets
from dagster import resource
from loan.loan import DuckDB

@resource(config_schema={"vars": str})
def duckdb(init_context):
    return DuckDB(init_context.resource_config["vars"])

duckdb.localstack = duckdb.configured(
    {
        "vars": """
set s3_access_key_id='test':
set s3_secret_access_key='test':
set s3_endpoint='localhost:4566':
set s3_use_ssl='false':
set s3_url_style='path':
"""
    }
)

@io_manager(required_resource_keys={"duckdb"})
def loan_io_manager(init_context):
    return LoanIOManager("datalake", init_context.resources.duckdb)

@repository
def loan():
    return [
        with_resources(
            load_assets_from_package_module(assets),
            {"io_manager": loan_io_manager, "duckdb": duckdb.localstack}
        )
    ]