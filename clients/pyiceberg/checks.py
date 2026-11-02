# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with PyIceberg: checks.py <plan.json> <out.json>."""

import json
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
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


class Fail(Exception):
    """A check result of fail; the message is the note."""


def run_row(catalog, ref, run_id, *fields):
    """The row of ref with runId = run_id, projected to fields (dotted names select nested fields)."""
    table = catalog.load_table((ref["namespace"], ref["table"]))
    rows = table.scan(row_filter=EqualTo("runId", run_id), selected_fields=fields).to_arrow().to_pylist()
    if not rows:
        raise Fail(f"{ref['namespace']}.{ref['table']} has no row with runId = {run_id}.")
    return rows[0]


def exact(value):
    return value if isinstance(value, Decimal) else Decimal(value)  # Decimal(float) is exact


def identifiers(catalog, plan):
    f = plan["fixture"]
    run_row(catalog, plan["escaped"], f["run_id"], plan["escaped"]["column"])
    names = f["expect"]["names"]
    schema = catalog.load_table((f["table"]["namespace"], f["table"]["table"])).schema()
    missing = [n for n in names if n not in [field.name for field in schema.fields]]
    if missing:
        return {"status": "fail", "note": f"The schema has no column named {', '.join(missing)}."}
    row = run_row(catalog, f["table"], f["run_id"], *names)
    wrong = {n: row.get(n) for n, v in names.items() if row.get(n) != v}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def decimal(catalog, plan):
    f = plan["fixture"]
    decimals = f["expect"]["decimals"]
    row = run_row(catalog, f["table"], f["run_id"], *decimals)
    wrong = {n: row[n] if row[n] is None else format(exact(row[n]), "f") for n, v in decimals.items() if row[n] is None or exact(row[n]) != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def timestamps(catalog, plan):
    os.environ["TZ"] = "Asia/Kolkata"
    time.tzset()
    f = plan["fixture"]
    t = f["expect"]["timestamp"]
    actual = run_row(catalog, f["table"], f["run_id"], t["column"])[t["column"]]
    expected = datetime.fromisoformat(ts(t["value"]))
    if not isinstance(actual, datetime) or actual.tzinfo is None or actual != expected:
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r}."}
    return {"status": "ok"}


def nesting(catalog, plan):
    f = plan["fixture"]
    n, lst = f["expect"]["nested"], f["expect"]["list"]
    row = run_row(catalog, f["table"], f["run_id"], ".".join([n["column"], *n["path"]]), lst["column"])
    leaf = row[n["column"]]
    for key in n["path"]:
        leaf = leaf.get(key) if isinstance(leaf, dict) else None
    if leaf != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {leaf!r}."}
    actual = row[lst["column"]] or []
    same = len(actual) == len(lst["value"]) and all(
        a["position"] is not None and exact(a["position"]) == Decimal(e["position"]) and a["label"] == e["label"]
        for a, e in zip(actual, lst["value"]))
    if not same:
        got = [{k: str(v) for k, v in a.items()} for a in actual]
        return {"status": "fail", "note": f"Expected the list {lst['value']}, got {got}."}
    return {"status": "ok"}


def large_values(catalog, plan):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    text = run_row(catalog, f["table"], f["run_id"], large["column"])[large["column"]] or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    trash = catalog.load_table((f["trash"]["namespace"], f["trash"]["table"]))
    rows = trash.scan(row_filter=EqualTo("id", f["event"]), selected_fields=("path", "data")).to_arrow().to_pylist()
    blobs = [r["data"] for r in rows if (r["path"] or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and a path ending in {binned['path_suffix']}."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(catalog, plan):
    f = plan["fixture"]
    e = f["expect"]["evolution"]
    run = run_row(catalog, f["table"], f["run_id"], e["column"])[e["column"]]
    baseline = run_row(catalog, f["table"], e["baseline_run"], e["column"])[e["column"]]
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def views(catalog, plan):
    v = plan["view"]
    catalog.load_view((v["namespace"], v["view"]))
    return {"status": "na", "note": "PyIceberg loads a view's metadata and SQL text but has no engine to run it (py.iceberg.apache.org/api/#load-a-view)."}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution}


def failure(e):
    if isinstance(e, Fail):
        return {"status": "fail", "note": redact(str(e))[:300]}
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
