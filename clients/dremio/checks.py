# /// script
# requires-python = ">=3.11"
# ///
"""Runs the contract checks through the DataHub REST API: checks.py <plan.json> <out.json>."""

import base64
import json
import sys
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from example import AUTH, BASE, TENANT, query, query_typed  # noqa: E402
from harness.redact import redact  # noqa: E402

# Tested through Cumulocity DataHub, which runs Dremio Enterprise and reads the catalog inside the platform;
# these cells state how DataHub's provisioning configures that Dremio source.
LOGIN = ("Tested through Cumulocity DataHub, whose provisioning gives the Dremio source an OAuth2 client "
         "credential for the catalog.")
VENDED = "Tested through Cumulocity DataHub, whose provisioning sets the Dremio source to use vended storage credentials."

# shortcut: a tenant user may read neither sys.version nor any other Dremio system table, so the Dremio Enterprise
# release comes from the DataHub version that bundles it (dremio-ee-utils.version in datahub-console's
# gradle.properties for that build); add a line when DataHub upgrades, until DataHub exposes Dremio's version.
DREMIO_FOR_DATAHUB = {"14.0.760": "26.1.12"}


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


def fixture_rows(plan, select, run_ids):
    t = plan["fixture"]["table"]
    ids = ", ".join(f"'{r}'" for r in run_ids)
    return query(f'SELECT {select} FROM {TENANT}.{t["namespace"]}."{t["table"]}" f WHERE f."runId" IN ({ids})')


def run_row(plan, select, typed=False):
    """The run's row of the fixture table: the row every fixture check reads; with typed, also its column types."""
    t, run_id = plan["fixture"]["table"], plan["fixture"]["run_id"]
    rows, types = query_typed(f'SELECT {select} FROM {TENANT}.{t["namespace"]}."{t["table"]}" f WHERE f."runId" = \'{run_id}\'')
    if not rows:
        raise LookupError(f"{t['table']} returns no row with runId {run_id}")
    return (rows[0], types) if typed else rows[0]


def identifiers(plan):
    e, f = plan["escaped"], plan["fixture"]
    escaped = query(f'SELECT x."{e["column"]}" FROM {TENANT}.{e["namespace"]}."{e["table"]}" x WHERE x."{e["column"]}" = \'{f["run_id"]}\'')
    names = f["expect"]["names"]
    # Each column addressed by its exact, quoted name, the way a customer's SQL has to.
    row = run_row(plan, ", ".join(f'f."{name}" AS "{name}"' for name in names))
    data = {"escaped_rows": len(escaped), "names": {name: row.get(name, "<missing>") for name in names}}
    if not escaped:
        return {"status": "fail", "data": data, "note": f"The escaped table {e['table']} returns no row for the run."}
    wrong = {name: value for name, value in data["names"].items() if value != names[name]}
    if wrong:
        return {"status": "fail", "data": data, "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": data}


def decimal(plan):
    expect = plan["fixture"]["expect"]["decimals"]
    row, types = run_row(plan, ", ".join(f'f."{c}"' for c in expect), typed=True)
    data = {c: str(row[c]) for c in expect}
    # The JSON literal alone cannot tell a decimal from a double printed in its shortest form; the type can.
    wrong = {c: f"{data[c]} ({types.get(c, {}).get('name')})" for c in expect
             if types.get(c, {}).get("name") != "DECIMAL" or not isinstance(row[c], Decimal) or row[c] != Decimal(expect[c])}
    if wrong:
        return {"status": "fail", "data": data, "note": f"Expected {({c: expect[c] for c in wrong})}, got {wrong}."}
    return {"status": "ok", "data": data,
            "note": "The REST API returns decimal(38,9) as exact JSON number literals, read here as decimals; "
                    "a JSON parser that reads numbers as doubles rounds them."}


def timestamps(plan):
    t = plan["fixture"]["expect"]["timestamp"]
    row, types = run_row(plan, f'f."{t["column"]}"', typed=True)
    got, kind = row[t["column"]], types.get(t["column"], {}).get("name")
    data = {"value": got, "type": kind}
    if kind != "TIMESTAMP":
        return {"status": "fail", "data": data, "note": f"Expected a TIMESTAMP column, got {kind}."}
    instant = datetime.fromisoformat(got)
    instant = instant if instant.tzinfo else instant.replace(tzinfo=timezone.utc)  # Dremio returns UTC without a zone
    if instant != datetime.fromisoformat(t["value"]):
        return {"status": "fail", "data": data, "note": f"Expected {t['value']}, got {got} (UTC)."}
    return {"status": "ok", "data": data,
            "note": "The REST API has no session time zone, so Asia/Kolkata could not be set; it returns the instant as UTC."}


def nesting(plan):
    n, lst = plan["fixture"]["expect"]["nested"], plan["fixture"]["expect"]["list"]
    leaf = ".".join(f'"{p}"' for p in [n["column"], *n["path"]])
    row = run_row(plan, f'f.{leaf} AS leaf, f."{lst["column"]}"')
    want = [{**e, "position": Decimal(e["position"])} for e in lst["value"]]
    got = row[lst["column"]]
    data = {"leaf": row["leaf"], "list": [{k: str(v) for k, v in e.items()} for e in got or []]}
    if row["leaf"] != n["value"]:
        return {"status": "fail", "data": data, "note": f"Expected the leaf {n['value']!r}, got {row['leaf']!r}."}
    if got != want:
        return {"status": "fail", "data": data, "note": f"Expected {lst['value']}, got {data['list']}."}
    return {"status": "ok", "data": data}


def large_values(plan):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    text = run_row(plan, f'f."{large["column"]}"')[large["column"]]
    trash = query(f"SELECT t.path, t.data FROM {TENANT}.{f['trash']['namespace']}.{f['trash']['table']} t WHERE t.id = '{f['event']}'")
    binary = [base64.b64decode(r["data"]) for r in trash if r["path"].endswith(binned["path_suffix"])]  # VARBINARY comes as base64
    data = {"chars": len(text), "bytes": len(text.encode()), "only_char": set(text) == {large["char"]},
            "binned": binary[0].decode("utf-8", "replace") if binary else None}
    if (data["chars"], data["bytes"], data["only_char"]) != (large["chars"], large["bytes"], True):
        return {"status": "fail", "data": data,
                "note": f"Expected {large['chars']} chars, {large['bytes']} bytes of {large['char']}, got {data['chars']} chars, {data['bytes']} bytes."}
    if data["binned"] != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {data['binned']!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(plan):
    f = plan["fixture"]
    ev = f["expect"]["evolution"]
    rows = {r["runId"]: r[ev["column"]] for r in fixture_rows(plan, f'f."runId", f."{ev["column"]}"', [f["run_id"], ev["baseline_run"]])}
    data = {"run": rows.get(f["run_id"], "<missing>"), "baseline": rows.get(ev["baseline_run"], "<missing>")}
    if ev["baseline_run"] not in rows:
        return {"status": "inconclusive", "data": data, "note": f"The baseline row {ev['baseline_run']} is not in the table."}
    if (data["run"], data["baseline"]) != (ev["run"], None):
        return {"status": "fail", "data": data, "note": f"Expected run {ev['run']!r} and baseline NULL, got {data['run']!r} and {data['baseline']!r}."}
    return {"status": "ok", "data": data}


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
        "catalog-login": {"status": "ok", "note": LOGIN},
        "s3": {"status": "ok", "note": VENDED},
        "cdc-tables": run(cdc_tables, plan),
        "latest-tables": run(latest_tables, plan),
        "identifiers": run(identifiers, plan),
        "views": run(views, plan),
        "decimal": run(decimal, plan),
        "timestamps": run(timestamps, plan),
        "nesting": run(nesting, plan),
        "large-values": run(large_values, plan),
        "schema-evolution": run(schema_evolution, plan),
    }
    request = urllib.request.Request(BASE.removesuffix("/dremio/api/v3") + "/version",
                                     headers={"Authorization": AUTH, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        datahub = json.load(response)["versionId"]
    version = DREMIO_FOR_DATAHUB.get(datahub, f"unknown (DataHub {datahub})")
    out = {"client": "dremio", "version": version, "build": f"through Cumulocity DataHub {datahub} REST API",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")
    for name, result in checks.items():
        print(f"{name}: {result['status']}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
