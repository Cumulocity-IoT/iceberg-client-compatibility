# /// script
# requires-python = ">=3.11"
# ///
"""Runs the contract checks (clients/CONTRACT.md) with clickhouse local: checks.py <plan.json> <out.json>."""

import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

from example import query  # noqa: E402
from harness.redact import redact  # noqa: E402

DOCS = "clickhouse.com/docs/engines/table-engines/integrations/iceberg"
REST_CATALOG = "src/Databases/DataLake/RestCatalog.cpp"


class Fail(Exception):
    """A check result of fail; the message is the note."""


def scrub(text):
    secret = os.environ.get("CATALOG_CREDENTIAL", "")
    for value in [secret, *secret.split(":")]:
        if len(value) >= 4:
            text = text.replace(value, "REDACTED")
    return redact(text)


def failure(e):
    if isinstance(e, Fail):
        return {"status": "fail", "note": scrub(str(e))[:300]}
    return {"status": "fail", "note": scrub(f"{type(e).__name__}: {' '.join(str(e).split())}")[:400]}


def table(ref, name_key="table"):
    return f"lake.`{ref['namespace']}.{ref[name_key]}`"


def ts(iso):
    return f"parseDateTime64BestEffort('{iso}', 6, 'UTC')"


def exact(value):
    return value if isinstance(value, Decimal) else Decimal(value)  # Decimal(float) and Decimal(int) are exact


def catalog_login(plan):
    rows, _ = query("SHOW TABLES FROM lake")
    listed = {r["name"].split(".", 1)[0] for r in rows}
    # ClickHouse lists tables, not namespaces, so a namespace that holds only views cannot appear.
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped")} - listed)
    if missing:
        return {"status": "fail", "note": f"SHOW TABLES FROM lake lists no table of {', '.join(missing)}."}
    result = {"status": "ok", "data": {"namespaces": len(listed)}}
    if plan["view"]["namespace"] not in listed:
        result["note"] = (f"ClickHouse lists the tables of each namespace, not the namespaces, so "
                          f"{plan['view']['namespace']}, which holds only a view, is not listed.")
    return result


def s3(plan):
    rows, _ = query(f"SELECT * FROM {table(plan['measurement'])} LIMIT 5")
    if not rows:
        return {"status": "fail", "note": "The measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": len(rows)}}


def cdc_tables(plan):
    m, w = plan["measurement"], plan["window"]
    device = f"source = '{m['source']}'"
    where = {
        "total": "1",
        "device": device,
        "not_device": f"NOT ({device})",
        "before": f"time < {ts(w['boundary'])}",
        "after": f"time >= {ts(w['boundary'])}",
    }
    # One query, so all counts read the same snapshot; each branch keeps its own filter for pruning.
    sql = " UNION ALL ".join(
        f"SELECT '{k}' AS k, count() AS n FROM {table(m)} WHERE time >= {ts(w['from'])} AND time < {ts(w['to'])} AND ({v})"
        for k, v in where.items())
    rows, _ = query(sql)
    d = {r["k"]: r["n"] for r in rows}
    d = {k: d[k] for k in where}
    if d["device"] + d["not_device"] != d["total"] or d["before"] + d["after"] != d["total"]:
        return {"status": "fail", "data": d, "note": "The filtered counts do not add up to the total."}
    if min(d["device"], d["before"], d["after"]) == 0:
        return {"status": "inconclusive", "data": d, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": d}


def latest_tables(plan):
    # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
    rows, _ = query(f"SELECT count() AS rows, count(DISTINCT id) AS ids FROM {table(plan['latest'])}")
    d = {"rows": rows[0]["rows"], "ids": rows[0]["ids"]}
    if d["rows"] == 0:
        return {"status": "inconclusive", "data": d, "note": "The latest table is empty."}
    if d["rows"] != d["ids"]:
        return {"status": "fail", "data": d, "note": f"{d['rows']} rows for {d['ids']} distinct ids, so deleted rows are returned."}
    return {"status": "ok", "data": d}


def run_row(plan, select, run_id=None, **settings):
    """The row of the fixture table with runId = run_id (default: the run's), and its column types."""
    f = plan["fixture"]
    run_id = run_id or f["run_id"]
    rows, types = query(f"SELECT {select} FROM {table(f['table'])} WHERE runId = '{run_id}'", **settings)
    if not rows:
        raise Fail(f"{f['table']['namespace']}.{f['table']['table']} returned no row with runId = {run_id}.")
    return rows[0], types


def identifiers(plan):
    e, f = plan["escaped"], plan["fixture"]
    rows, types = query(f"SELECT `{e['column']}` FROM {table(e)} WHERE `{e['column']}` = '{f['run_id']}'")
    if list(types) != [e["column"]]:
        return {"status": "fail", "note": f"The column came back as {list(types)}, not as {e['column']}."}
    if not rows:
        return {"status": "fail", "note": f"The escaped table returned no row with {e['column']} = {f['run_id']}."}
    names = f["expect"]["names"]
    row, types = run_row(plan, ", ".join(f"`{n}`" for n in names))
    if list(types) != list(names):
        return {"status": "fail", "note": f"The columns came back as {list(types)}, not as {list(names)}."}
    wrong = [f"{n}: expected {v!r}, got {row[n]!r}" for n, v in names.items() if row[n] != v]
    if wrong:
        return {"status": "fail", "note": "; ".join(wrong) + "."}
    return {"status": "ok", "data": {"columns": len(names)}}


def decimal(plan):
    decimals = plan["fixture"]["expect"]["decimals"]
    row, types = run_row(plan, ", ".join(f"`{n}`" for n in decimals))
    wrong = [f"{n}: expected {v}, got {row[n]!r} as {types[n]}" for n, v in decimals.items()
             if "Decimal(38, 9)" not in types[n] or not isinstance(row[n], (Decimal, int)) or exact(row[n]) != Decimal(v)]
    if wrong:
        return {"status": "fail", "note": "; ".join(wrong) + "."}
    return {"status": "ok"}


def timestamps(plan):
    t = plan["fixture"]["expect"]["timestamp"]
    c = t["column"]
    row, types = run_row(plan, f"`{c}`, toUnixTimestamp64Milli(`{c}`) AS millis", session_timezone="Asia/Kolkata")
    instant = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
    expected = (instant - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
    data = {"value": row[c], "type": types[c]}
    if "DateTime64" not in types[c]:
        return {"status": "fail", "data": data, "note": f"Expected a DateTime64 column, got {types[c]}."}
    if row["millis"] != expected:
        actual = datetime.fromtimestamp(row["millis"] / 1000, timezone.utc).isoformat() if row["millis"] is not None else None
        return {"status": "fail", "data": data, "note": f"Expected the instant {t['value']}, got {actual}."}
    return {"status": "ok", "data": data}


def nesting(plan):
    ex = plan["fixture"]["expect"]
    n, lst = ex["nested"], ex["list"]
    leaf = ".".join(f"`{p}`" for p in [n["column"], *n["path"]])
    row, _ = run_row(plan, f"{leaf} AS leaf, `{lst['column']}`")
    if row["leaf"] != n["value"]:
        return {"status": "fail", "note": f"The leaf: expected {n['value']!r}, got {row['leaf']!r}."}
    actual = row[lst["column"]]
    same = isinstance(actual, list) and len(actual) == len(lst["value"]) and all(
        isinstance(a, dict) and a.keys() == e.keys() and isinstance(a["position"], (Decimal, int))
        and exact(a["position"]) == Decimal(e["position"]) and a["label"] == e["label"]
        for a, e in zip(actual, lst["value"]))
    if not same:
        return {"status": "fail", "note": f"The list: expected {lst['value']}, got {actual}."}
    return {"status": "ok"}


def large_values(plan):
    f = plan["fixture"]
    g, b = f["expect"]["large"], f["expect"]["binned"]
    row, _ = run_row(plan, f"`{g['column']}`")
    text = row[g["column"]] or ""
    got = (len(text), len(text.encode("utf-8")), set(text))
    if got != (g["chars"], g["bytes"], {g["char"]}):
        return {"status": "fail", "note": f"Expected {g['chars']} chars, {g['bytes']} bytes, all {g['char']!r}; "
                f"got {got[0]} chars, {got[1]} bytes, characters {sorted(got[2])[:5]}."}
    # Iceberg binary is a ClickHouse String of raw bytes; hex() carries them through JSON unchanged.
    rows, _ = query(f"SELECT path, hex(data) AS data FROM {table(f['trash'])} WHERE id = '{f['event']}'")
    binned = [r["data"] for r in rows if (r["path"] or "").endswith(b["path_suffix"])]
    if not binned:
        return {"status": "fail", "note": f"The trash table has no row with id = {f['event']} and that path ({len(rows)} rows for the id)."}
    data = bytes.fromhex(binned[0]).decode("utf-8")
    if data != b["data"]:
        return {"status": "fail", "note": f"The binned data: expected {b['data']!r}, got {data[:80]!r}."}
    return {"status": "ok", "data": {"chars": got[0], "bytes": got[1]}}


def schema_evolution(plan):
    v = plan["fixture"]["expect"]["evolution"]
    run, _ = run_row(plan, f"`{v['column']}`")
    baseline, _ = run_row(plan, f"`{v['column']}`", v["baseline_run"])
    if run[v["column"]] != v["run"] or baseline[v["column"]] is not None:
        return {"status": "fail", "note": f"Expected {v['run']!r} on the run's row and NULL on the baseline row, "
                f"got {run[v['column']]!r} and {baseline[v['column']]!r}."}
    return {"status": "ok"}


def views(plan):
    m, v, w = plan["measurement"], plan["view"], plan["window"]
    where = f"WHERE source = '{m['source']}' AND time >= {ts(w['from'])} AND time < {ts(w['to'])}"
    try:
        view = query(f"SELECT count() AS n FROM {table(v, 'view')} {where}")[0][0]["n"]
    except RuntimeError as e:
        if "UNKNOWN_TABLE" not in str(e):
            raise
        return {"status": "na", "note": f"ClickHouse's Iceberg REST catalog loads only tables, never views ({REST_CATALOG}), "
                                        "so the view is an unknown table (UNKNOWN_TABLE)."}
    d = {"view": view, "table": query(f"SELECT count() AS n FROM {table(m)} {where}")[0][0]["n"]}
    if d["view"] != d["table"]:
        return {"status": "fail", "data": d, "note": f"The view returns {d['view']} rows, its base table {d['table']}."}
    if d["table"] == 0:
        return {"status": "inconclusive", "data": d, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": d}


def incremental_read(plan):
    return {"status": "na", "note": f"ClickHouse reads an Iceberg table at one snapshot (iceberg_snapshot_id, "
                                    f"iceberg_timestamp_ms) and has no incremental read between snapshots ({DOCS})."}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    checks = {}
    for name, check in CHECKS.items():
        try:
            checks[name] = check(plan)
        except Exception as e:  # a failing check must not stop the others
            checks[name] = failure(e)
        print(f"{name}: {checks[name]['status']}", file=sys.stderr)
    clickhouse = os.environ.get("CLICKHOUSE", "clickhouse")  # asked without the catalog, so a failed login still reports it
    version = subprocess.run([clickhouse, "local", "--query", "SELECT version()"], capture_output=True, text=True,
                             check=True).stdout.strip()
    out = {"client": "clickhouse", "version": version,
           "build": f"clickhouse local {version} (DataLakeCatalog database engine), release binary for "
                    f"{platform.system()} {platform.machine()}",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
