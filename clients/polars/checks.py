# /// script
# requires-python = ">=3.11"
# dependencies = ["polars[iceberg]==2.1.0", "polars-iceberg==0.1.0", "pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with Polars' scan_iceberg: checks.py <plan.json> <out.json>."""

import json
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import polars as pl
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.snapshots import Operation

from example import connect
from harness.redact import redact

TZ = "Asia/Kolkata"


def instant(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def scan(catalog, ref, **options):
    return pl.scan_iceberg(catalog.load_table((ref["namespace"], ref["table"])), **options)


def catalog_login(catalog, plan):
    listed = {".".join(ns) for ns in catalog.list_namespaces()}
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
    if missing:
        return {"status": "fail", "note": f"list_namespaces does not list {', '.join(missing)}."}
    return {"status": "ok"}


def s3(catalog, plan):
    rows = scan(catalog, plan["measurement"]).head(5).collect().height
    if rows == 0:
        return {"status": "fail", "note": "The scan of the measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": rows}}


def cdc_tables(catalog, plan):
    m, w = plan["measurement"], plan["window"]
    lf, t = scan(catalog, m), pl.col("time")
    in_window = (t >= instant(w["from"])) & (t < instant(w["to"]))
    device = pl.col("source") == m["source"]
    where = {"total": True, "device": device, "not_device": ~device,
             "before": t < instant(w["boundary"]), "after": t >= instant(w["boundary"])}
    data = {k: lf.filter(in_window & v).select(pl.len()).collect().item() for k, v in where.items()}
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(catalog, plan):
    try:
        # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
        data = scan(catalog, plan["latest"]).select(rows=pl.len(), ids=pl.col("id").n_unique()).collect().row(0, named=True)
    except ValueError as e:
        if "does not yet support equality deletes" not in str(e):
            raise
        return {"status": "na", "note": "The polars-iceberg planner rejects equality delete files and PyIceberg's reader, the fallback, raises 'PyIceberg does not yet support equality deletes' (apache/iceberg-python#1210)."}
    if data["rows"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    if data["rows"] != data["ids"]:
        return {"status": "fail", "data": data, "note": "The scan returns more rows than distinct ids, so deleted rows come back."}
    return {"status": "ok", "data": data}


class Fail(Exception):
    """A check result of fail; the message is the note."""


def run_row(catalog, ref, run_id, *columns):
    """The run's row of ref, projected to columns (names or expressions), as a one-row DataFrame."""
    df = scan(catalog, ref).filter(pl.col("runId") == run_id).select(*columns).collect()
    if df.height == 0:
        raise Fail(f"{ref['namespace']}.{ref['table']} has no row with runId = {run_id}.")
    return df.head(1)


def exact(value):
    """The value if it arrived as a Decimal, else None: a float or a string does not pass."""
    return value if isinstance(value, Decimal) else None


def identifiers(catalog, plan):
    f = plan["fixture"]
    run_row(catalog, plan["escaped"], f["run_id"], plan["escaped"]["column"])
    names = f["expect"]["names"]
    schema = scan(catalog, f["table"]).collect_schema().names()
    missing = [n for n in names if n not in schema]
    if missing:
        return {"status": "fail", "note": f"The schema has no column named {', '.join(missing)}."}
    row = run_row(catalog, f["table"], f["run_id"], *names).row(0, named=True)
    wrong = {n: row[n] for n, v in names.items() if row[n] != v}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def decimal(catalog, plan):
    f = plan["fixture"]
    decimals = f["expect"]["decimals"]
    row = run_row(catalog, f["table"], f["run_id"], *decimals).row(0, named=True)
    wrong = {n: repr(row[n]) for n, v in decimals.items() if exact(row[n]) != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def timestamps(catalog, plan):
    os.environ["TZ"] = TZ
    time.tzset()
    f = plan["fixture"]
    t = f["expect"]["timestamp"]
    raw = run_row(catalog, f["table"], f["run_id"], t["column"])
    if not isinstance(raw.dtypes[0], pl.Datetime) or raw.dtypes[0].time_zone is None:  # a naive value is no instant
        return {"status": "fail", "note": f"Expected a time-zone-aware Datetime, got {raw.dtypes[0]}."}
    df = raw.select(pl.col(t["column"]).dt.convert_time_zone(TZ))
    actual = df.item()
    if df.dtypes[0] != pl.Datetime(time_zone=TZ) or not isinstance(actual, datetime) or actual != instant(t["value"]):
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r} as {df.dtypes[0]}."}
    return {"status": "ok"}


def nesting(catalog, plan):
    f = plan["fixture"]
    n, lst = f["expect"]["nested"], f["expect"]["list"]
    leaf = pl.col(n["column"])
    for key in n["path"]:
        leaf = leaf.struct.field(key)
    row = run_row(catalog, f["table"], f["run_id"], leaf.alias("leaf"), lst["column"]).row(0, named=True)
    if row["leaf"] != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {row['leaf']!r}."}
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
    text = run_row(catalog, f["table"], f["run_id"], large["column"]).item() or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    blobs = (scan(catalog, f["trash"]).filter((pl.col("id") == f["event"]) & pl.col("path").str.ends_with(binned["path_suffix"]))
             .select("data").collect()["data"].to_list())
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and a path ending in {binned['path_suffix']}."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(catalog, plan):
    f = plan["fixture"]
    e = f["expect"]["evolution"]
    run = run_row(catalog, f["table"], f["run_id"], e["column"]).item()
    baseline = run_row(catalog, f["table"], e["baseline_run"], e["column"]).item()
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def incremental_read(catalog, plan):
    f = plan["fixture"]
    table = catalog.load_table((f["table"]["namespace"], f["table"]["table"]))
    posted = instant(f["posted"]).timestamp() * 1000
    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)
    before = [s for s in snapshots if s.timestamp_ms < posted]
    after = [s for s in snapshots if s.timestamp_ms > posted and s.summary and s.summary.operation == Operation.APPEND]
    if not before or not after:
        return {"status": "inconclusive", "note": f"The fixture table has no snapshot before {f['posted']} or no append after it."}
    start, end = before[-1], after[0]
    runs = (pl.scan_iceberg(table, from_snapshot_id_exclusive=start.snapshot_id, to_snapshot_id_inclusive=end.snapshot_id)
            .select("runId").collect()["runId"].to_list())
    data = {"from": str(start.snapshot_id), "to": str(end.snapshot_id), "rows": len(runs)}
    if runs == [f["run_id"]]:
        return {"status": "ok", "data": data}
    added = int(end.summary["added-records"])  # what the snapshot metadata says the append added
    if f["run_id"] in runs and len(runs) == added:
        return {"status": "inconclusive", "data": data, "note": f"The first append after the post added {added} rows per its snapshot summary, the run's row and {sorted(set(runs) - {f['run_id']})[:3]}, and Polars returned exactly those."}
    return {"status": "fail", "data": data, "note": f"Expected the one row {f['run_id']} (the snapshot summary says {added} added), got {runs[:5]}."}


def views(catalog, plan):
    v, m, w = plan["view"], plan["measurement"], plan["window"]
    catalog.load_view((v["namespace"], v["view"]))  # the view exists in the catalog
    t = pl.col("time")
    rows = (t >= instant(w["from"])) & (t < instant(w["to"])) & (pl.col("source") == m["source"])
    ctx = pl.SQLContext().register_catalog("lake", catalog)
    try:
        on_view = ctx.execute(f'SELECT * FROM lake."{v["namespace"]}"."{v["view"]}"').filter(rows).select(pl.len()).collect().item()
    except NoSuchTableError:
        return {"status": "na", "note": "Polars reads Iceberg tables only: scan_iceberg and SQLContext.register_catalog load a name with load_table, which raises NoSuchTableError for the view (docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html)."}
    data = {"view": on_view, "table": scan(catalog, m).filter(rows).select(pl.len()).collect().item()}
    if data["view"] != data["table"]:
        return {"status": "fail", "data": data, "note": f"The view returns {data['view']} rows, its base table {data['table']}."}
    if data["table"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": data}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


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
    libs = {p: version(p) for p in ("polars", "polars-iceberg", "pyiceberg", "pyarrow")}
    out = {"client": "polars", "version": libs["polars"],
           "build": ", ".join(f"{p} {v}" for p, v in libs.items()) + "; scan_iceberg with its default reader: Polars' native"
                    " Parquet reader, scans planned by the polars-iceberg plugin, PyIceberg's reader only where native falls back",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
