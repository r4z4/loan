from duckdb import connect
from string import Template
from sqlescapy import sqlescape
import pandas as pd
from typing import Mapping
from dagster import IOManager

class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings

class DuckDB:
    def __init__(self, options=""):
        self.options = options
    
    def collect_dataframes(self, s: SQL) -> Mapping[str, pd.DataFrame]:
        dataframes = {}
        for key, value in s.bindings.items():
            if isinstance(value, pd.DataFrame):
                dataframes[f"df_{id(value)}"] = value
            elif isinstance(value, SQL):
                dataframes.update(self.collect_dataframes(value))
        return dataframes
    
    def query(self, select_statement: SQL):
        print(select_statement)
        db = connect(":memory:")
        db.query("install httpfs; load httpfs;")
        db.query(self.options)

        dataframes = self.collect_dataframes(select_statement)
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(self.sql_to_string(select_statement))
        if result is None:
            return
        return result.df()

    def sql_to_string(self, s: SQL) -> str:
        replacements = {}
        for key, value in s.bindings.items():
            if isinstance(value, pd.DataFrame):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, SQL):
                replacements[key] = f"({self.sql_to_string(value)})"
            elif isinstance(value, str):
                replacements[key] = f"'{sqlescape(value)}'"
            elif isinstance(value, (int, float, bool)):
                replacements[key] = str(value)
            elif value is None:
                replacements[key] = f"({sql_to_string(value)})"
            else:
                raise ValueError(f"Invalid type for {key}")
        return Template(s.sql).safe_substitute(replacements)


class LoanIOManager(IOManager):
    def __init__(self, bucket_name: str, duckdb: DuckDB, prefix=""):
        self.bucket_name = bucket_name
        self.duckdb = duckdb
        self.prefix = prefix

    def _get_s3_url(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"s3://{self.bucket_name}/{self.prefix}{'/'.join(id)}.parquet"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return
        if not isinstanceof(select_statement, SQL):
            raise ValueError(
                r"Expected asset to return a SQL; got {select_statement!r}"
            )
        self.duckdb.query(
            SQL(
                "copy $select_statement to $url (format parquet)",
                select_statement=select_statement,
                url=self._get_s3_url(context),
            )
        )

    def loan_input(self, context) -> SQL:
        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))