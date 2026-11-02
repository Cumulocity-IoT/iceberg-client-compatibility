# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql==1.2.3"]
# ///
"""Registers the lake of a Cumulocity tenant as the external catalog `lake` in StarRocks and reads a few rows.

Start StarRocks, for example `docker run -p 9030:9030 starrocks/allin1-ubuntu:4.1.7`, export the CATALOG_*
settings and run `uv run --script example.py`. STARROCKS_HOST and STARROCKS_PORT name the server's MySQL
port (default 127.0.0.1:9030).
"""

import os

import pymysql


def connect():
    return pymysql.connect(
        host=os.environ.get("STARROCKS_HOST", "127.0.0.1"),
        port=int(os.environ.get("STARROCKS_PORT", "9030")),
        user=os.environ.get("STARROCKS_USER", "root"),
        password=os.environ.get("STARROCKS_PASSWORD", ""),
        autocommit=True,
    )


def create_catalog(cursor, name="lake", **extra):
    """Registers the tenant's Iceberg REST catalog; the values travel as escaped parameters, never in a log line."""
    properties = {
        "type": "iceberg",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": os.environ["CATALOG_URI"],
        "iceberg.catalog.warehouse": os.environ["CATALOG_WAREHOUSE"],
        "iceberg.catalog.security": "oauth2",
        "iceberg.catalog.oauth2.credential": os.environ["CATALOG_CREDENTIAL"],  # clientId:clientSecret
        "iceberg.catalog.oauth2.server-uri": os.environ["CATALOG_TOKEN_URI"],
        "iceberg.catalog.oauth2.scope": os.environ["CATALOG_SCOPE"],
        # Sends X-Iceberg-Access-Delegation: vended-credentials and reads the data files with those credentials.
        "iceberg.catalog.vended-credentials-enabled": "true",
        **extra,
    }
    pairs = ", ".join(["%s = %s"] * len(properties))
    cursor.execute(f"CREATE EXTERNAL CATALOG IF NOT EXISTS `{name}` PROPERTIES ({pairs})",
                   [part for item in properties.items() for part in item])


if __name__ == "__main__":
    with connect() as connection, connection.cursor() as cursor:
        create_catalog(cursor)
        cursor.execute("SELECT * FROM lake.cdc_measurement.c8y_Temperature LIMIT 5")
        for row in cursor.fetchall():
            print(row)
