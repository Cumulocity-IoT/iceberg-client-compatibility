# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.0", "pytz==2026.6"]  # DuckDB converts TIMESTAMPTZ to datetime with pytz
# ///
"""Runs the contract checks (clients/CONTRACT.md) with DuckDB SQL: checks.py <plan.json> <out.json>."""

import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import duckdb  # noqa: E402
import pytz  # noqa: E402

from example import connect  # noqa: E402
from harness.redact import redact  # noqa: E402


class Fail(Exception):
    """A check result of fail; the message is the note."""


def name(*parts):
    return "lake." + ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def ts(iso):
    return f"TIMESTAMPTZ '{iso}'"


def rows(con, sql):
    return con.sql(sql).fetchall()


def catalog_login(con, plan):
    listed = {r[0] for r in rows(con, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake'")}
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
    if missing:
        return {"status": "fail", "note": f"The attached catalog does not list {', '.join(missing)}."}
    return {"status": "ok", "data": {"namespaces": len(listed)}}


def s3(con, plan):
    m = plan["measurement"]
    n = len(rows(con, f"SELECT * FROM {name(m['namespace'], m['table'])} LIMIT 5"))
    if n == 0:
        return {"status": "fail", "note": "The measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": n}}


def cdc_tables(con, plan):
    m, w = plan["measurement"], plan["window"]
    device = f"source = '{m['source']}'"
    where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})",
             "before": f"time < {ts(w['boundary'])}", "after": f"time >= {ts(w['boundary'])}"}
    base = f"SELECT count(*) FROM {name(m['namespace'], m['table'])} WHERE time >= {ts(w['from'])} AND time < {ts(w['to'])}"
    d = {k: rows(con, f"{base} AND ({v})")[0][0] for k, v in where.items()}
    if d["device"] + d["not_device"] != d["total"] or d["before"] + d["after"] != d["total"]:
        return {"status": "fail", "data": d, "note": "The filtered counts do not add up to the total."}
    if min(d["device"], d["before"], d["after"]) == 0:
        return {"status": "inconclusive", "data": d, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": d}


def latest_tables(con, plan):
    t = plan["latest"]
    # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
    total, ids = rows(con, f"SELECT count(*), count(DISTINCT id) FROM {name(t['namespace'], t['table'])}")[0]
    d = {"rows": total, "ids": ids}
    if total == 0:
        return {"status": "inconclusive", "data": d, "note": "The latest table returned no rows."}
    if total != ids:
        return {"status": "fail", "data": d, "note": f"{total} rows for {ids} distinct ids, so deleted rows come back."}
    return {"status": "ok", "data": d}


def run_row(con, plan, select, run_id=None):
    """The row of the fixture table with runId = run_id (the plan's run by default), as a dict."""
    t, run_id = plan["fixture"]["table"], run_id or plan["fixture"]["run_id"]
    rel = con.sql(f"""SELECT {select} FROM {name(t['namespace'], t['table'])} f WHERE f."runId" = '{run_id}'""")
    found = rel.fetchall()
    if not found:
        raise Fail(f"{t['namespace']}.{t['table']} has no row with runId = {run_id}.")
    return dict(zip(rel.columns, found[0]))


def exact(value):
    return value if isinstance(value, Decimal) else None  # a float or a string is not an exact decimal


def identifiers(con, plan):
    e, f = plan["escaped"], plan["fixture"]
    rel = con.sql(f"""SELECT "{e['column']}" FROM {name(e['namespace'], e['table'])} WHERE "{e['column']}" = '{f['run_id']}'""")
    if not rel.fetchall():
        return {"status": "fail", "note": f"The escaped table {e['table']} returned no row with {e['column']} = {f['run_id']}."}
    names = f["expect"]["names"]
    row = run_row(con, plan, ", ".join(f'f."{n}"' for n in names))
    if list(row) != list(names):
        return {"status": "fail", "note": f"The columns came back as {list(row)}, not as {list(names)}."}
    wrong = {n: row[n] for n, v in names.items() if row[n] != v}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {"columns": len(names)}}


def decimal(con, plan):
    decimals = plan["fixture"]["expect"]["decimals"]
    row = run_row(con, plan, ", ".join(f'f."{n}"' for n in decimals))
    wrong = {n: repr(row[n]) for n, v in decimals.items() if exact(row[n]) is None or row[n] != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def timestamps(con, plan):
    t = plan["fixture"]["expect"]["timestamp"]
    con.execute("SET TimeZone = 'Asia/Kolkata'")
    actual = run_row(con, plan, f'f."{t["column"]}"')[t["column"]]
    expected = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
    if not isinstance(actual, datetime) or actual.tzinfo is None or actual != expected:
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r}."}
    return {"status": "ok", "data": {"value": actual.isoformat()}}


def nesting(con, plan):
    n, lst = plan["fixture"]["expect"]["nested"], plan["fixture"]["expect"]["list"]
    leaf = "f." + ".".join(f'"{p}"' for p in [n["column"], *n["path"]])  # DuckDB's own struct field access
    row = run_row(con, plan, f'{leaf} AS leaf, f."{lst["column"]}"')
    if row["leaf"] != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {row['leaf']!r}."}
    actual = row[lst["column"]] or []
    same = len(actual) == len(lst["value"]) and all(
        a.keys() == e.keys() and exact(a["position"]) == Decimal(e["position"]) and a["label"] == e["label"]
        for a, e in zip(actual, lst["value"]))
    if not same:
        got = [{k: repr(v) for k, v in a.items()} for a in actual]
        return {"status": "fail", "note": f"Expected the list {lst['value']}, got {got}."}
    return {"status": "ok"}


def large_values(con, plan):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    text = run_row(con, plan, f'f."{large["column"]}"')[large["column"]] or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    t = f["trash"]
    found = rows(con, f"SELECT path, data FROM {name(t['namespace'], t['table'])} WHERE id = '{f['event']}'")
    blobs = [d for p, d in found if (p or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and a path ending in {binned['path_suffix']}."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(con, plan):
    e = plan["fixture"]["expect"]["evolution"]
    run = run_row(con, plan, f'f."{e["column"]}"')[e["column"]]
    baseline = run_row(con, plan, f'f."{e["column"]}"', e["baseline_run"])[e["column"]]
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def incremental_read(con, plan):
    f = plan["fixture"]
    table = name(f["table"]["namespace"], f["table"]["table"])
    posted = datetime.fromisoformat(f["posted"]).astimezone(timezone.utc).replace(tzinfo=None)  # snapshot timestamp_ms is UTC without a zone
    snaps = rows(con, f"SELECT snapshot_id, timestamp_ms, operation FROM iceberg_snapshots({table}) ORDER BY timestamp_ms")
    start = [s for s, at, _ in snaps if at < posted]
    end = [s for s, at, op in snaps if at > posted and op == "append"]
    if not start or not end:
        return {"status": "inconclusive", "note": "The fixture table has no snapshot before or no append snapshot after the fixture was posted."}
    try:
        found = rows(con, f"""SELECT "runId" FROM iceberg_scan('{table.replace('"', "")}', start_snapshot_id = {start[-1]}, end_snapshot_id = {end[0]})""")
    except duckdb.BinderException as e:
        if "Invalid named parameter" not in str(e):
            raise
        return {"status": "na", "note": "iceberg_scan reads one snapshot and has no start/end snapshot range "
                f"({str(e).splitlines()[0][:120]}); incremental reads are an open request (duckdb/duckdb-iceberg#1355)."}
    if [r[0] for r in found] != [f["run_id"]]:
        return {"status": "fail", "data": {"rows": len(found)}, "note": f"Expected only the run's row, got {len(found)} rows: {[r[0] for r in found][:5]}."}
    return {"status": "ok", "data": {"rows": 1}}


def views(con, plan):
    v, m, w = plan["view"], plan["measurement"], plan["window"]
    where = f"WHERE source = '{m['source']}' AND time >= {ts(w['from'])} AND time < {ts(w['to'])}"
    try:
        view = rows(con, f"SELECT count(*) FROM {name(v['namespace'], v['view'])} {where}")[0][0]
    except duckdb.BinderException as e:
        if "no SQL representation with dialect 'duckdb'" not in str(e):
            raise
        # fail, not na: DuckDB reads Iceberg views, but only in its own dialect, which these views lack.
        return {"status": "fail", "note": "DuckDB runs only an Iceberg view's 'duckdb' SQL representation and these views carry "
                f"DremioSQL, spark, trino and starrocks ({str(e).splitlines()[0][:120]})."}
    table = rows(con, f"SELECT count(*) FROM {name(m['namespace'], m['table'])} {where}")[0][0]
    d = {"view": view, "table": table}
    if view != table:
        return {"status": "fail", "data": d, "note": f"The view returns {view} rows, its base table {table}."}
    if table == 0:
        return {"status": "inconclusive", "data": d, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": d}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


def scrub(text):
    for value in [os.environ.get("CATALOG_CREDENTIAL", ""), *os.environ.get("CATALOG_CREDENTIAL", "").split(":")]:
        if len(value) >= 4:
            text = text.replace(value, "REDACTED")
    return redact(text)


def failure(e):
    if isinstance(e, Fail):
        return {"status": "fail", "note": scrub(str(e))[:300]}
    first_line = (str(e).strip().splitlines() or [""])[0]
    return {"status": "fail", "note": scrub(f"{type(e).__name__}: {first_line}")[:300]}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    try:
        con = connect()
        checks = {}
    except Exception as e:  # the login itself failed, so no check can run
        con, checks = None, {check_id: failure(e) for check_id in CHECKS}
    for check_id, check in CHECKS.items():
        if con is None:
            break
        try:
            checks[check_id] = check(con, plan)
        except Exception as e:  # a failing check must not stop the others
            checks[check_id] = failure(e)
        print(f"{check_id}: {checks[check_id]['status']}", file=sys.stderr)
    extensions = dict(rows(con, "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded "
                                "AND extension_name IN ('iceberg', 'httpfs', 'avro')")) if con else {}
    # The extensions are the ones built for this DuckDB release, installed from its extension repository.
    build = [f"duckdb {duckdb.__version__}", *(f"{k} extension {v}" for k, v in sorted(extensions.items())),
             f"pytz {pytz.__version__}"]
    out = {"client": "duckdb", "version": duckdb.__version__, "build": ", ".join(build), "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
