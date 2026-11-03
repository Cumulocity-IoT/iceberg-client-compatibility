# /// script
# requires-python = ">=3.11"
# dependencies = ["snowflake-connector-python==5.1.0"]
# ///
"""Query the Streaming Lake Ingestion lake from Snowflake and print a few rows.

The lake is a catalog-linked database over the tenant's Iceberg REST catalog: its namespaces are schemas and its
tables are tables. The catalog is case-sensitive, so every schema, table and column name is double-quoted in its
exact case. Settings: SNOWFLAKE_ACCOUNT (<org>-<account>), SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY (the base64 body
of an unencrypted PKCS#8 private key, the PEM without its BEGIN/END lines), SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE
and SNOWFLAKE_DATABASE (the catalog-linked database).
"""

import base64
import os

import snowflake.connector

_connection = None


def connect():
    """One key-pair authenticated session, with the catalog-linked database as its current database."""
    global _connection
    if _connection is None:
        _connection = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            authenticator="SNOWFLAKE_JWT",
            private_key=base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY"]),  # PKCS#8 DER
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
    return _connection


def query(sql):
    """Runs one statement and returns its rows as tuples and its columns (cursor.description: name, type_code, ...)."""
    with connect().cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall(), cursor.description


if __name__ == "__main__":
    rows, _ = query('SELECT m."source", m."time", m."T":"value" AS t FROM "cdc_measurement"."c8y_Temperature" m LIMIT 5')
    for row in rows:
        print(row)
