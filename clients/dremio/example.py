# /// script
# requires-python = ">=3.11"
# ///
"""Query the Streaming Lake Ingestion lake through Cumulocity DataHub (Dremio) and print a few rows.

DataHub reads the tenant's Iceberg catalog inside the platform; its Dremio source is named after the
tenant id, so a table is addressed as <tenant>.<namespace>.<table>.
"""

import base64
import json
import os
import time
import urllib.request
from decimal import Decimal

BASE = os.environ["C8Y_BASEURL"].rstrip("/") + "/service/datahub/dremio/api/v3"
TENANT = os.environ["C8Y_TENANT"]
AUTH = "Basic " + base64.b64encode(
    f"{TENANT}/{os.environ['C8Y_USER']}:{os.environ['C8Y_PASSWORD']}".encode()
).decode()


def _call(path, body=None):
    request = urllib.request.Request(
        BASE + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": AUTH, "Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        # DataHub writes decimal(38,9) as exact JSON number literals; a float would round them.
        return json.load(response, parse_float=Decimal)


def query(sql, context=None, timeout=600):
    """Runs one SQL query as a DataHub job and returns its rows as a list of dicts."""
    return query_typed(sql, context, timeout)[0]


def query_typed(sql, context=None, timeout=600):
    """Like query, and also returns the result's column types by name, for example {"n": {"name": "BIGINT"}}."""
    job = _call("/sql", {"sql": sql, **({"context": context} if context else {})})["id"]
    deadline = time.monotonic() + timeout
    while (state := _call(f"/job/{job}"))["jobState"] not in ("COMPLETED", "FAILED", "CANCELED"):
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job} still {state['jobState']} after {timeout}s")
        time.sleep(1)
    if state["jobState"] != "COMPLETED":
        raise RuntimeError(f"job {job} {state['jobState']}: {state.get('errorMessage', '')}")
    rows, types = [], {}
    while len(rows) < state.get("rowCount", 0):  # results come in pages of at most 500 rows
        page = _call(f"/job/{job}/results?offset={len(rows)}&limit=500")
        rows += page["rows"]
        types = {c["name"]: c["type"] for c in page.get("schema", [])}
    return rows, types


if __name__ == "__main__":
    # "time" is a reserved word in Dremio SQL; struct fields need a table alias.
    for row in query(f'SELECT m.source, m."time", m.T."value" AS t FROM {TENANT}.cdc_measurement.c8y_Temperature m LIMIT 5'):
        print(row)
