# /// script
# requires-python = ">=3.11"
# dependencies = ["daft==0.7.26", "pyiceberg==0.11.1", "pyarrow==25.0.1"]
# ///
"""Connects Daft to the lake of a Cumulocity tenant and reads a few rows.

Daft reads Iceberg tables that PyIceberg loads from the catalog. Run with `uv run --script example.py` after
exporting the CATALOG_* settings.
"""

import os

import daft
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
    daft.read_iceberg(table).limit(5).show()
