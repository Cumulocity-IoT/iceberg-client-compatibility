# /// script
# requires-python = ">=3.11"
# dependencies = ["polars[iceberg]==2.1.0", "polars-iceberg==0.1.0", "pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""Reads a few rows of the lake of a Cumulocity tenant with Polars, through the tenant's Iceberg catalog.

Polars loads the table through a PyIceberg catalog. Run with `uv run --script example.py` after exporting the
CATALOG_* settings.
"""

import os

import polars as pl
from pyiceberg.catalog.rest import RestCatalog


def connect(**extra):
    return RestCatalog(
        "lake",
        **{
            "uri": os.environ["CATALOG_URI"],
            "warehouse": os.environ["CATALOG_WAREHOUSE"],
            "credential": os.environ["CATALOG_CREDENTIAL"],  # clientId:clientSecret
            "oauth2-server-uri": os.environ["CATALOG_TOKEN_URI"],
            "scope": os.environ["CATALOG_SCOPE"],
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            **extra,
        },
    )


if __name__ == "__main__":
    table = connect().load_table(("cdc_measurement", "c8y_Temperature"))
    print(pl.scan_iceberg(table).head(5).collect())
