# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""Connects PyIceberg to the lake of a Cumulocity tenant and reads a few rows.

Run with `uv run --script example.py` after exporting the CATALOG_* settings.
"""

import os

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
    print(table.scan(limit=5).to_arrow().to_pylist())
