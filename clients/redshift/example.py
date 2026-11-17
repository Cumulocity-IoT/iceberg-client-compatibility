# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3==1.44.4"]
# ///
"""Query the Streaming Lake Ingestion lakehouse with Amazon Redshift Serverless, through AWS Glue catalog federation.

aws/setup.sh federates the tenant's catalog into the AWS Glue Data Catalog with CREDENTIAL_VENDING_MODE =
SOURCE_VENDED, links each namespace into the default Glue catalog, and creates an external schema of the same name on
each link in the workgroup's database, so a table is addressed as <namespace>.<table>. Glue federates only the tables
whose names have no capitals, such as latest_inventory.inventory. The queries go through the Redshift Data API as the
caller's IAM identity. Settings: REDSHIFT_WORKGROUP and REDSHIFT_DATABASE (default compat).
"""

import os
import time

import boto3

WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "compat")
DATABASE = os.environ.get("REDSHIFT_DATABASE", "compat")


def session():
    """The AWS identity that calls Redshift: boto3's default, or PLATFORM_AWS_* as the tests pass it."""
    keys = {f"aws_{k.lower()}": os.environ[f"PLATFORM_AWS_{k}"]
            for k in ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN") if os.environ.get(f"PLATFORM_AWS_{k}")}
    return boto3.Session(**keys, region_name=os.environ.get("PLATFORM_AWS_REGION"))


def query(sql, *settings, timeout=600):
    """Runs the settings (such as SET timezone) and then sql in one session; returns the columns of sql as
    (label, type) and its rows, every value as the Data API delivers it."""
    data = session().client("redshift-data")
    target = {"WorkgroupName": WORKGROUP, "Database": DATABASE}
    if settings:
        statement = data.batch_execute_statement(Sqls=[*settings, sql], **target)["Id"]
        result = f"{statement}:{len(settings) + 1}"
    else:
        statement = result = data.execute_statement(Sql=sql, **target)["Id"]
    deadline = time.monotonic() + timeout
    while (status := data.describe_statement(Id=statement))["Status"] not in ("FINISHED", "FAILED", "ABORTED"):
        if time.monotonic() > deadline:
            data.cancel_statement(Id=statement)
            raise TimeoutError(f"statement {statement} still {status['Status']} after {timeout}s")
        time.sleep(1)
    if status["Status"] != "FINISHED":
        raise RuntimeError(status.get("Error", status["Status"]))
    columns, rows = None, []
    for page in data.get_paginator("get_statement_result").paginate(Id=result):
        columns = columns or [(c["label"], c["typeName"]) for c in page["ColumnMetadata"]]
        rows += [[None if field.get("isNull") else next(iter(field.values())) for field in record] for record in page["Records"]]
    return columns, rows


if __name__ == "__main__":
    columns, rows = query("SELECT id, name, lastupdated FROM latest_inventory.inventory LIMIT 5")
    for row in rows:
        print(dict(zip((label for label, _ in columns), row)))
