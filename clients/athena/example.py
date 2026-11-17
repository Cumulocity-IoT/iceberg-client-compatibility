# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3==1.44.4"]
# ///
"""Query the Streaming Lake Ingestion lakehouse with Amazon Athena, through AWS Glue catalog federation.

aws/setup.sh federates the tenant's catalog into the AWS Glue Data Catalog as the catalog GLUE_CATALOG (by default
the tenant id), with the connection property CREDENTIAL_VENDING_MODE = SOURCE_VENDED, so that Athena reads the data
files with the catalog's vended credentials. Glue federates only the tables whose names have no capitals, such as
latest_inventory.inventory. Settings: ATHENA_WORKGROUP (default compat) and GLUE_CATALOG.
"""

import os
import time

import boto3

WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "compat")
CATALOG = os.environ.get("GLUE_CATALOG") or os.environ["CATALOG_WAREHOUSE"]


def session():
    """The AWS identity that calls Athena: boto3's default, or PLATFORM_AWS_* as the tests pass it."""
    keys = {f"aws_{k.lower()}": os.environ[f"PLATFORM_AWS_{k}"]
            for k in ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN") if os.environ.get(f"PLATFORM_AWS_{k}")}
    return boto3.Session(**keys, region_name=os.environ.get("PLATFORM_AWS_REGION"))


def query(sql, timeout=600):
    """Runs one query in the workgroup; returns the columns as (name, type) and the rows, every value as text."""
    athena = session().client("athena")
    run = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP,
                                       QueryExecutionContext={"Catalog": CATALOG})["QueryExecutionId"]
    deadline = time.monotonic() + timeout
    while (status := athena.get_query_execution(QueryExecutionId=run)["QueryExecution"]["Status"])["State"] in ("QUEUED", "RUNNING"):
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=run)
            raise TimeoutError(f"query {run} still {status['State']} after {timeout}s")
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(status.get("StateChangeReason", status["State"]))
    columns, rows = None, []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=run):
        result = page["ResultSet"]
        if columns is None:  # the first row of the first page holds the column labels
            columns = [(c["Name"], c["Type"]) for c in result["ResultSetMetadata"]["ColumnInfo"]]
            result["Rows"] = result["Rows"][1:]
        rows += [[d.get("VarCharValue") for d in row["Data"]] for row in result["Rows"]]
    return columns, rows


if __name__ == "__main__":
    columns, rows = query("SELECT id, name, lastupdated FROM latest_inventory.inventory LIMIT 5")
    for row in rows:
        print(dict(zip((name for name, _ in columns), row)))
