# /// script
# requires-python = ">=3.11"
# dependencies = ["trino==0.340.0"]
# ///
"""Reads a few rows of the lake through a Trino server whose catalog `lake` is example.properties.

Run with `uv run --script example.py`; TRINO_HOST and TRINO_PORT name the server (default localhost:8080).
"""

import os

import trino


def connect(catalog="lake", **session):
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "trino"),
        catalog=catalog,
        **session,
    )


if __name__ == "__main__":
    cursor = connect().cursor()
    cursor.execute('SELECT * FROM cdc_measurement."c8y_Temperature" LIMIT 5')
    print(cursor.fetchall())
