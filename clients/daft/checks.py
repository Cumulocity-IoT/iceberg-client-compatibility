# /// script
# requires-python = ">=3.11"
# dependencies = ["daft==0.7.26", "pyiceberg==0.11.1", "pyarrow==25.0.1"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with Daft: checks.py <plan.json> <out.json>."""

import json
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import daft
import pyarrow
import pyiceberg
from daft import col, lit
from daft.catalog import Catalog

from example import connect
from harness.redact import redact


def instant(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def read(catalog, ref):
    return daft.read_iceberg(catalog.load_table((ref["namespace"], ref["table"])))


def catalog_login(catalog, plan):
    listed = {".".join(ns) for ns in catalog.list_namespaces()}
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
    if missing:
        return {"status": "fail", "note": f"list_namespaces does not list {', '.join(missing)}."}
    return {"status": "ok"}


def s3(catalog, plan):
    rows = len(read(catalog, plan["measurement"]).limit(5).to_pylist())
    if rows == 0:
        return {"status": "fail", "note": "The read of the measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": rows}}


def in_window(plan):
    w = plan["window"]
    return (col("time") >= lit(instant(w["from"]))) & (col("time") < lit(instant(w["to"])))


def cdc_tables(catalog, plan):
    m, w = plan["measurement"], plan["window"]
    df = read(catalog, m)
    window, device, boundary = in_window(plan), col("source") == lit(m["source"]), lit(instant(w["boundary"]))

    def count(condition):
        return df.where(window & condition).count_rows()

    data = {"total": df.where(window).count_rows(), "device": count(device), "not_device": count(~device),
            "before": count(col("time") < boundary), "after": count(col("time") >= boundary)}
    if data["device"] + data["not_device"] < data["total"]:
        return {"status": "fail", "data": data, "note": f"NOT (source = S) counts {data['not_device']} of the {data['total'] - data['device']} rows of other devices, which matches Eventual-Inc/Daft#7657, a comparison under NOT rewritten onto the source bucket partition."}
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(catalog, plan):
    t = plan["latest"]
    table = catalog.load_table((t["namespace"], t["table"]))
    summary = table.current_snapshot().summary
    deletes = {k: int(summary.get(f"total-{k.replace('_', '-')}") or 0) for k in ("equality_deletes", "position_deletes")}
    try:
        # One aggregation reads one snapshot; two could straddle an upsert of this continuously updated table.
        row = daft.read_iceberg(table).agg(col("id").count("all").alias("rows"), col("id").count_distinct().alias("ids")).to_pylist()[0]
    except (ValueError, NotImplementedError) as e:
        if "does not yet support equality deletes" not in str(e):
            raise
        return {"status": "na", "data": deletes, "note": "Daft plans Iceberg reads with PyIceberg, which raises on equality delete files; Daft applies position deletes only (Eventual-Inc/Daft#2458, equality deletes blocked by PyIceberg)."}
    data = {**row, **deletes}
    if data["rows"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    if data["rows"] != data["ids"]:
        return {"status": "fail", "data": data, "note": "The read returns more rows than distinct ids, so deleted rows come back."}
    return {"status": "ok", "data": data}


class Fail(Exception):
    """A check result of fail; the message is the note."""


def run_row(catalog, ref, run_id, **columns):
    """The row of ref with runId = run_id, as {name: value} for the given {name: expression}."""
    df = read(catalog, ref).where(col("runId") == lit(run_id))
    rows = df.select(*(e.alias(n) for n, e in columns.items())).to_pylist()
    if not rows:
        raise Fail(f"{ref['namespace']}.{ref['table']} has no row with runId = {run_id}.")
    return rows[0]


def identifiers(catalog, plan):
    f, e = plan["fixture"], plan["escaped"]
    run_row(catalog, e, f["run_id"], **{e["column"]: col(e["column"])})
    names = f["expect"]["names"]
    schema = read(catalog, f["table"]).column_names
    missing = [n for n in names if n not in schema]
    if missing:
        return {"status": "fail", "note": f"The schema has no column named {', '.join(missing)}."}
    row = run_row(catalog, f["table"], f["run_id"], **{n: col(n) for n in names})
    wrong = {n: row[n] for n, v in names.items() if row[n] != v}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def is_decimal(value, expected):
    return isinstance(value, Decimal) and value == Decimal(expected)  # a float or a string does not pass


def decimal(catalog, plan):
    f = plan["fixture"]
    decimals = f["expect"]["decimals"]
    row = run_row(catalog, f["table"], f["run_id"], **{n: col(n) for n in decimals})
    wrong = {n: f"{row[n]!r} ({type(row[n]).__name__})" for n, v in decimals.items() if not is_decimal(row[n], v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def timestamps(catalog, plan):
    os.environ["TZ"] = "Asia/Kolkata"
    time.tzset()
    f = plan["fixture"]
    t = f["expect"]["timestamp"]
    actual = run_row(catalog, f["table"], f["run_id"], v=col(t["column"]))["v"]
    if not isinstance(actual, datetime) or actual.tzinfo is None or actual != instant(t["value"]):
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r}."}
    return {"status": "ok"}


def nesting(catalog, plan):
    f = plan["fixture"]
    n, lst = f["expect"]["nested"], f["expect"]["list"]
    leaf = col(n["column"])
    for key in n["path"]:
        leaf = leaf[key]  # Daft's struct field access
    row = run_row(catalog, f["table"], f["run_id"], leaf=leaf, list=col(lst["column"]))
    if row["leaf"] != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {row['leaf']!r}."}
    actual = row["list"] or []
    same = len(actual) == len(lst["value"]) and all(
        is_decimal(a["position"], e["position"]) and a["label"] == e["label"] for a, e in zip(actual, lst["value"]))
    if not same:
        got = [{k: f"{v!r}" for k, v in a.items()} for a in actual]
        return {"status": "fail", "note": f"Expected the list {lst['value']}, got {got}."}
    return {"status": "ok"}


def large_values(catalog, plan):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    text = run_row(catalog, f["table"], f["run_id"], v=col(large["column"]))["v"] or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    rows = read(catalog, f["trash"]).where(col("id") == lit(f["event"])).select("path", "data").to_pylist()
    blobs = [r["data"] for r in rows if (r["path"] or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and a path ending in {binned['path_suffix']}."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(catalog, plan):
    f = plan["fixture"]
    e = f["expect"]["evolution"]
    run = run_row(catalog, f["table"], f["run_id"], v=col(e["column"]))["v"]
    baseline = run_row(catalog, f["table"], e["baseline_run"], v=col(e["column"]))["v"]
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def views(catalog, plan):
    v, m = plan["view"], plan["measurement"]
    session = daft.Session()
    session.attach_catalog(Catalog.from_iceberg(catalog), alias="lake")  # session-local, nothing is written
    device = in_window(plan) & (col("source") == lit(m["source"]))
    counts = {}
    for key, ns, name in (("base", m["namespace"], m["table"]), ("view", v["namespace"], v["view"])):
        try:
            counts[key] = session.sql(f'SELECT * FROM lake."{ns}"."{name}"').where(device).count_rows()
        except Exception as e:
            if key == "base" or "not found" not in str(e).lower() or not catalog.view_exists((ns, name)):
                raise
            note = "Daft's catalogs resolve only tables and namespaces, and its Iceberg catalog loads names with PyIceberg's load_table, so the view is not found (docs.daft.ai/en/stable/connectors/custom-catalogs)"
            return {"status": "na", "data": counts, "note": redact(f"{note}: {type(e).__name__}: {first_line(e)}")[:400]}
    if counts["view"] != counts["base"] or counts["base"] == 0:
        return {"status": "fail", "data": counts, "note": f"The view counts {counts['view']} rows for the device, its base table {counts['base']}."}
    return {"status": "ok", "data": counts}


def incremental_read(catalog, plan):
    return {"status": "na", "note": "Daft's read_iceberg reads one snapshot, branch or tag and has no snapshot-range read (Eventual-Inc/Daft#4139, open)."}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


def first_line(e):
    return (str(e).strip().splitlines() or [""])[0]


def failure(e):
    if isinstance(e, Fail):
        return {"status": "fail", "note": redact(str(e))[:300]}
    return {"status": "fail", "note": redact(f"{type(e).__name__}: {first_line(e)}")[:300]}


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
    out = {"client": "daft", "version": daft.__version__,
           "build": f"daft {daft.__version__}, pyiceberg {pyiceberg.__version__}, pyarrow {pyarrow.__version__}",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
