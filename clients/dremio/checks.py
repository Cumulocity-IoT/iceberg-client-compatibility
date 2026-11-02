# /// script
# requires-python = ">=3.11"
# ///
"""Runs the contract checks through the DataHub REST API: checks.py <plan.json> <out.json>."""

import json
import sys
import urllib.request
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from example import AUTH, BASE, TENANT, query  # noqa: E402
from harness.redact import redact  # noqa: E402

INSIDE = "DataHub connects to the catalog inside the platform, not through the public endpoint"


def ts(iso):
    return f"TIMESTAMP '{iso.replace('T', ' ').rstrip('Z')}'"  # Dremio reads timestamptz as UTC timestamps


def count(sql):
    return query(sql)[0]["n"]


def cdc_tables(plan):
    m, w = plan["measurement"], plan["window"]
    base = f'SELECT COUNT(*) AS n FROM {TENANT}.{m["namespace"]}.{m["table"]} m WHERE m."time" >= {ts(w["from"])} AND m."time" < {ts(w["to"])}'
    device = f"m.source = '{m['source']}'"
    data = {
        "total": count(base),
        "device": count(f"{base} AND {device}"),
        "not_device": count(f"{base} AND NOT ({device})"),
        "before": count(f'{base} AND m."time" < {ts(w["boundary"])}'),
        "after": count(f'{base} AND m."time" >= {ts(w["boundary"])}'),
    }
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(plan):
    t = plan["latest"]
    row = query(f'SELECT COUNT(*) AS "rows", COUNT(DISTINCT id) AS ids FROM {TENANT}.{t["namespace"]}.{t["table"]}')[0]
    data = {"rows": row["rows"], "ids": row["ids"]}
    if data["rows"] != data["ids"]:
        return {"status": "fail", "data": data, "note": "The latest table returns more rows than distinct ids."}
    if data["rows"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    return {"status": "ok", "data": data}


def identifiers(plan):
    e = plan["escaped"]
    table = f'{TENANT}.{e["namespace"]}."{e["table"]}"'
    rows = query(f'SELECT x."{e["column"]}" FROM {table} x LIMIT 10')
    listed = query(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{TENANT}.{e['namespace']}' AND TABLE_NAME = '{e['table']}'"
    )
    data = {"rows": len(rows), "column_found": e["column"] in {c["COLUMN_NAME"] for c in listed}}
    if not data["column_found"]:
        return {"status": "fail", "data": data, "note": f"The column {e['column']} is not listed under its exact name."}
    if rows:
        return {"status": "ok", "data": data}
    # Tell an empty table from one whose rows Dremio fails to read.
    if count(f"SELECT COUNT(*) AS n FROM TABLE(table_snapshot('{table}'))") == 0:
        return {"status": "inconclusive", "data": data, "note": "The escaped table has no snapshot yet, so it holds no rows to read."}
    return {"status": "fail", "data": data, "note": "The escaped table has snapshots but returns no rows."}


def views(plan):
    m, v, w = plan["measurement"], plan["view"], plan["window"]
    where = f"""WHERE x.source = '{m["source"]}' AND x."time" >= {ts(w["from"])} AND x."time" < {ts(w["to"])}"""
    data = {
        "view": count(f'SELECT COUNT(*) AS n FROM {TENANT}.{v["namespace"]}.{v["view"]} x {where}'),
        "table": count(f'SELECT COUNT(*) AS n FROM {TENANT}.{m["namespace"]}.{m["table"]} x {where}'),
    }
    if data["view"] != data["table"]:
        return {"status": "fail", "data": data, "note": "The view and its base table return different counts."}
    if data["view"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device."}
    return {"status": "ok", "data": data}


def run(check, plan):
    try:
        return check(plan)
    except Exception as error:  # a check that raises is a genuine failure of the query
        return {"status": "fail", "note": redact(f"{type(error).__name__}: {error}")[:500]}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    checks = {
        "catalog-login": {"status": "na", "note": INSIDE},
        "s3": {"status": "na", "note": INSIDE},
        "cdc-tables": run(cdc_tables, plan),
        "latest-tables": run(latest_tables, plan),
        "identifiers": run(identifiers, plan),
        "views": run(views, plan),
    }
    # The row is DataHub, so its version is DataHub's; a tenant user may not read Dremio's sys.version.
    request = urllib.request.Request(BASE.removesuffix("/dremio/api/v3") + "/version",
                                     headers={"Authorization": AUTH, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        version = json.load(response)["versionId"]
    out = {"client": "dremio", "version": version, "build": "DataHub REST API", "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")
    for name, result in checks.items():
        print(f"{name}: {result['status']}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
