# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.0"]
# ///
"""Connects DuckDB to the lake of a Cumulocity tenant and reads a few rows.

Run with `uv run --script example.py` after exporting the CATALOG_* settings. The iceberg extension is installed
on first use, in the version built for this DuckDB release.
"""

import os

import duckdb


def connect():
    con = duckdb.connect()
    client_id, client_secret = os.environ["CATALOG_CREDENTIAL"].split(":", 1)
    con.execute(
        "CREATE SECRET lake_secret (TYPE iceberg, CLIENT_ID ?, CLIENT_SECRET ?, OAUTH2_SERVER_URI ?, OAUTH2_SCOPE ?)",
        [client_id, client_secret, os.environ["CATALOG_TOKEN_URI"], os.environ["CATALOG_SCOPE"]],
    )
    # Vended credentials are the default access delegation mode of the iceberg extension.
    con.execute(f"ATTACH '{os.environ['CATALOG_WAREHOUSE']}' AS lake (TYPE iceberg, URI '{os.environ['CATALOG_URI']}', SECRET lake_secret)")
    return con


if __name__ == "__main__":
    print(connect().sql("SELECT * FROM lake.cdc_measurement.c8y_Temperature LIMIT 5"))
