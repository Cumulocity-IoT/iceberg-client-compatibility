# /// script
# requires-python = ">=3.11"
# ///
"""Connects clickhouse local to the lake of a Cumulocity tenant and reads a few rows.

Run with `uv run --script example.py` after exporting the CATALOG_* settings and CLICKHOUSE, the path of the
clickhouse binary. The SQL goes to clickhouse local on stdin, so the client secret stays off the command line.
A table is addressed as lake.`<namespace>.<table>`.
"""

import json
import os
import subprocess
from decimal import Decimal


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def connect_sql():
    """The statements that open the tenant's catalog as the session-local database `lake`."""
    return f"""
SET allow_database_iceberg = 1;
CREATE DATABASE lake ENGINE = DataLakeCatalog({literal(os.environ["CATALOG_URI"])})
SETTINGS catalog_type = 'rest',
    warehouse = {literal(os.environ["CATALOG_WAREHOUSE"])},
    catalog_credential = {literal(os.environ["CATALOG_CREDENTIAL"])},
    oauth_server_uri = {literal(os.environ["CATALOG_TOKEN_URI"])},
    auth_scope = {literal(os.environ["CATALOG_SCOPE"])},
    vended_credentials = true;
"""


def query(sql, **settings):
    """Runs one query against the lake; returns its rows as dicts and its column types by name."""
    # 64-bit integers unquoted: ClickHouse's JSON format writes counts and epoch milliseconds as strings otherwise.
    command = [os.environ.get("CLICKHOUSE", "clickhouse"), "local", "--output_format_json_quote_64bit_integers=0",
               *(f"--{k}={v}" for k, v in settings.items())]
    run = subprocess.run(command, input=f"{connect_sql()}{sql} FORMAT JSON;", capture_output=True, text=True)
    if run.returncode:
        raise RuntimeError(run.stderr.strip() or f"clickhouse local exited with {run.returncode}")
    result = json.loads(run.stdout, parse_float=Decimal)  # decimal(38,9) arrives as an exact JSON number
    return result["data"], {column["name"]: column["type"] for column in result["meta"]}


if __name__ == "__main__":
    rows, _ = query("SELECT source, time, T.value FROM lake.`cdc_measurement.c8y_Temperature` LIMIT 5")
    for row in rows:
        print(row)
