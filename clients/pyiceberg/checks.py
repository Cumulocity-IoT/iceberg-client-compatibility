# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with PyIceberg: checks.py <plan.json> <out.json>."""

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import pyarrow
import pyarrow.compute as pc
import pyiceberg
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, Not

from example import connect
from harness.redact import redact


def ts(iso):
    return iso.replace("Z", "+00:00")  # PyIceberg parses timestamptz literals only with a numeric offset


def catalog_login(catalog, plan):
    listed = {".".join(ns) for ns in catalog.list_namespaces()}
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
    if missing:
        return {"status": "fail", "note": f"list_namespaces does not list {', '.join(missing)}."}
    return {"status": "ok"}


def s3(catalog, plan):
    m = plan["measurement"]
    rows = catalog.load_table((m["namespace"], m["table"])).scan(limit=1).to_arrow().num_rows
    if rows == 0:
        return {"status": "fail", "note": "The scan of the measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": rows}}


def cdc_tables(catalog, plan):
    m, w = plan["measurement"], plan["window"]
    table = catalog.load_table((m["namespace"], m["table"]))
    start, boundary, end = (GreaterThanOrEqual("time", ts(w["from"])), ts(w["boundary"]), LessThan("time", ts(w["to"])))
    device = EqualTo("source", m["source"])

    def count(*conditions):
        return table.scan(row_filter=And(*conditions), selected_fields=("source",)).to_arrow().num_rows

    data = {
        "total": count(start, end),
        "device": count(start, end, device),
        "not_device": count(start, end, Not(device)),
        "before": count(start, LessThan("time", boundary)),
        "after": count(GreaterThanOrEqual("time", boundary), end),
    }
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(catalog, plan):
    t = plan["latest"]
    table = catalog.load_table((t["namespace"], t["table"]))
    try:
        ids = table.scan(selected_fields=("id",)).to_arrow()["id"]
    except ValueError as e:
        if "does not yet support equality deletes" not in str(e):
            raise
        return {"status": "na", "note": "PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210)."}
    data = {"rows": len(ids), "ids": pc.count_distinct(ids).as_py()}
    if data["rows"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    if data["rows"] != data["ids"]:
        return {"status": "fail", "data": data, "note": "The scan returns more rows than distinct ids, so deleted rows come back."}
    return {"status": "ok", "data": data}


def identifiers(catalog, plan):
    e = plan["escaped"]
    table = catalog.load_table((e["namespace"], e["table"]))
    if e["column"] not in [f.name for f in table.schema().fields]:
        return {"status": "fail", "note": f"The schema has no column named {e['column']}."}
    result = table.scan(selected_fields=(e["column"],)).to_arrow()
    data = {"rows": result.num_rows}
    if result.column_names != [e["column"]]:
        return {"status": "fail", "data": data, "note": f"The scan returns the columns {result.column_names}."}
    if result.num_rows == 0:
        if table.current_snapshot() is None:
            return {"status": "inconclusive", "data": data, "note": "The table loads with the column under its exact name but has no snapshot, so there are no rows to read."}
        return {"status": "fail", "data": data, "note": "The scan returned no rows although the table has a snapshot."}
    return {"status": "ok", "data": data}


def views(catalog, plan):
    v = plan["view"]
    catalog.load_view((v["namespace"], v["view"]))
    return {"status": "na", "note": "PyIceberg loads a view's metadata and SQL text but has no engine to run it (py.iceberg.apache.org/api/#load-a-view)."}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views}


def failure(e):
    first_line = (str(e).strip().splitlines() or [""])[0]
    return {"status": "fail", "note": redact(f"{type(e).__name__}: {first_line}")[:300]}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    try:
        catalog = connect()
        checks = {}
    except Exception as e:  # the login itself failed, so no check can run
        catalog, checks = None, {name: failure(e) for name in CHECKS}
    for name, check in CHECKS.items():
        if catalog is None:
            break
        try:
            checks[name] = check(catalog, plan)
        except Exception as e:
            checks[name] = failure(e)
        print(f"{name}: {checks[name]['status']}", file=sys.stderr)
    out = {"client": "pyiceberg", "version": pyiceberg.__version__,
           "build": f"pyiceberg {pyiceberg.__version__}, pyarrow {pyarrow.__version__}", "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
