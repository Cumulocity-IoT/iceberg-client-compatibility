# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql==1.2.3"]
# ///
"""Connects Apache Doris to the lake of a Cumulocity tenant and reads a few rows.

Run with `uv run --script example.py` after exporting the CATALOG_* settings; DORIS_HOST and DORIS_PORT name the
frontend's MySQL port (default 127.0.0.1:9030).
"""

import os

import pymysql


def connect(catalog="c8y", **extra):
    """A MySQL-protocol session on Doris with the tenant's catalog registered as `catalog`."""
    conn = pymysql.connect(host=os.environ.get("DORIS_HOST", "127.0.0.1"), port=int(os.environ.get("DORIS_PORT", "9030")),
                           user=os.environ.get("DORIS_USER", "root"), password=os.environ.get("DORIS_PASSWORD", ""),
                           charset="utf8mb4", autocommit=True)
    properties = {
        "type": "iceberg",
        "iceberg.catalog.type": "rest",
        "iceberg.rest.uri": os.environ["CATALOG_URI"],
        "warehouse": os.environ["CATALOG_WAREHOUSE"],
        "iceberg.rest.security.type": "oauth2",
        "iceberg.rest.oauth2.credential": os.environ["CATALOG_CREDENTIAL"],  # clientId:clientSecret
        "iceberg.rest.oauth2.server-uri": os.environ["CATALOG_TOKEN_URI"],
        "iceberg.rest.oauth2.scope": os.environ["CATALOG_SCOPE"],
        "iceberg.rest.vended-credentials-enabled": "true",
        **extra,
    }
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}` PROPERTIES ("
                       + ", ".join(f"{conn.escape(k)} = {conn.escape(v)}" for k, v in properties.items()) + ")")
    return conn


if __name__ == "__main__":
    with connect().cursor() as cursor:
        cursor.execute("SELECT * FROM c8y.cdc_measurement.c8y_Temperature LIMIT 5")
        for row in cursor.fetchall():
            print(row)
