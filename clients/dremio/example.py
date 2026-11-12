# /// script
# requires-python = ">=3.11"
# ///
"""Query the Streaming Lake Ingestion lake with Dremio Enterprise's REST API and print a few rows.

The lake is the Dremio source Cumulocity provisions for the tenant, named after the tenant id, so a
table is addressed as <tenant>.<namespace>.<table>. Settings: DREMIO_URL (for example
https://datahub.<domain>:9447), DREMIO_USER, DREMIO_PASSWORD and C8Y_TENANT.
"""

import json
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal

BASE = os.environ["DREMIO_URL"].rstrip("/")
TENANT = os.environ["C8Y_TENANT"]
_token = None


def _request(path, body=None, auth=True):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if auth:
        headers["Authorization"] = token()
    request = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            # Dremio writes decimal(38,9) as exact JSON number literals; a float would round them.
            return json.load(response, parse_float=Decimal)
    except urllib.error.HTTPError as error:
        # Dremio states why it rejected a request in the body's errorMessage; the status line alone does not.
        detail = error.read().decode(errors="replace")
        try:
            detail = json.loads(detail).get("errorMessage") or detail
        except (ValueError, AttributeError):
            pass
        raise RuntimeError(f"HTTP {error.code} on {path.split('?')[0]}: {' '.join(str(detail).split())[:300]}") from None


def token():
    """The session token of a Dremio login, which the API takes as "_dremio<token>"."""
    global _token
    if _token is None:
        login = _request("/apiv2/login", {"userName": os.environ["DREMIO_USER"], "password": os.environ["DREMIO_PASSWORD"]}, auth=False)
        _token = "_dremio" + login["token"]
    return _token


def info():
    """Dremio's server information, its version among it."""
    return _request("/api/v3/info")


def query(sql, context=None, timeout=600):
    """Runs one SQL query as a Dremio job and returns its rows as a list of dicts."""
    return query_typed(sql, context, timeout)[0]


def query_typed(sql, context=None, timeout=600):
    """Like query, and also returns the result's column types by name, for example {"n": {"name": "BIGINT"}}."""
    job = _request("/api/v3/sql", {"sql": sql, **({"context": context} if context else {})})["id"]
    deadline = time.monotonic() + timeout
    while (state := _request(f"/api/v3/job/{job}"))["jobState"] not in ("COMPLETED", "FAILED", "CANCELED"):
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job} still {state['jobState']} after {timeout}s")
        time.sleep(1)
    if state["jobState"] != "COMPLETED":
        raise RuntimeError(f"job {job} {state['jobState']}: {state.get('errorMessage', '')}")
    rows, types = [], {}
    while len(rows) < state.get("rowCount", 0):  # results come in pages of at most 500 rows
        page = _request(f"/api/v3/job/{job}/results?offset={len(rows)}&limit=500")
        rows += page["rows"]
        types = {c["name"]: c["type"] for c in page.get("schema", [])}
    return rows, types


if __name__ == "__main__":
    # "time" is a reserved word in Dremio SQL; struct fields need a table alias.
    for row in query(f'SELECT m.source, m."time", m.T."value" AS t FROM {TENANT}.cdc_measurement.c8y_Temperature m LIMIT 5'):
        print(row)
