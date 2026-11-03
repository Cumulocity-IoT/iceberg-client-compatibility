# /// script
# requires-python = ">=3.11"
# dependencies = ["snowflake-connector-python==5.1.0"]
# ///
"""Runs the contract checks with Snowflake SQL on the catalog-linked database: checks.py <plan.json> <out.json>."""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import snowflake.connector  # noqa: E402
from snowflake.connector.constants import FIELD_ID_TO_NAME  # noqa: E402

from example import query  # noqa: E402
from harness.redact import redact  # noqa: E402

# One provisioning step sets up both: login and storage access are properties of the catalog integration behind the
# linked database, which the queries through it then prove.
SOURCE = "A Snowflake catalog integration was provisioned with the OAuth client credential and credential vending."

LINKED_DB = "docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database"
EQUALITY_DELETES = f'A catalog-linked database does not read equality delete files ("Row-level deletes with equality delete files aren\'t supported", {LINKED_DB})'
VIEWS = f"A catalog-linked database syncs the remote namespaces as schemas and their Iceberg tables, not Iceberg views ({LINKED_DB})"
INCREMENTAL = "Snowflake has no incremental read of an Iceberg table in a catalog-linked database"

KOLKATA = timedelta(hours=5, minutes=30)  # Asia/Kolkata has no daylight saving time


def message(error):
    text = " ".join(f"{type(error).__name__}: {error}".split())
    # The query and request ids differ on every run, and would change the footnote with them.
    text = re.sub(r"\b[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}: ", "", text)
    text = re.sub(r"\s*\(request_id=[^)]*\)", "", text)
    key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
    return redact(text.replace(key, "REDACTED") if len(key) >= 8 else text)[:400]


def name(ref, key="table"):
    return f'"{ref["namespace"]}"."{ref[key]}"'


def ts(iso):
    """An instant as a TIMESTAMP_TZ literal; with an explicit offset, the session time zone does not move it."""
    return f"'{datetime.fromisoformat(iso).isoformat()}'::TIMESTAMP_TZ"


def count(sql):
    return query(sql)[0][0][0]


def kind(column):
    return FIELD_ID_TO_NAME.get(column.type_code, str(column.type_code))


def in_window(plan, alias):
    w = plan["window"]
    return f'{alias}."time" >= {ts(w["from"])} AND {alias}."time" < {ts(w["to"])}'


def catalog_login(plan):
    rows, columns = query("SHOW SCHEMAS IN DATABASE")
    at = [c.name for c in columns].index("name")
    listed = {r[at] for r in rows}
    f = plan["fixture"]
    wanted = {plan[k]["namespace"] for k in ("measurement", "latest", "escaped")} | {f["table"]["namespace"], f["trash"]["namespace"]}
    missing = sorted(wanted - listed)
    if missing:
        return {"status": "fail", "note": f"SHOW SCHEMAS does not list {', '.join(missing)}."}
    return {"status": "ok", "data": {"schemas": len(listed)}, "note": SOURCE}


def s3(plan):
    rows, _ = query(f"SELECT * FROM {name(plan['measurement'])} LIMIT 5")
    if not rows:
        return {"status": "fail", "note": "The measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": len(rows)}, "note": SOURCE}


def cdc_tables(plan):
    m, b = plan["measurement"], ts(plan["window"]["boundary"])
    device = f"""m."source" = '{m["source"]}'"""
    where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})", "before": f'm."time" < {b}', "after": f'm."time" >= {b}'}
    data = {k: count(f"SELECT COUNT(*) FROM {name(m)} m WHERE {in_window(plan, 'm')} AND ({v})") for k, v in where.items()}
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(plan):
    try:
        # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
        rows, ids = query(f'SELECT COUNT(*), COUNT(DISTINCT "id") FROM {name(plan["latest"])}')[0][0]
    except Exception as error:
        if re.search(r"(?i)equality.?delete", str(error)):
            return {"status": "na", "note": f"{EQUALITY_DELETES}; the query fails with {message(error)}"}
        raise
    data = {"rows": rows, "ids": ids}
    if rows == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    if rows != ids:
        return {"status": "fail", "data": data, "note": f"{rows} rows for {ids} distinct ids, so deleted rows are returned."}
    return {"status": "ok", "data": data}


def views(plan):
    v, m = plan["view"], plan["measurement"]
    where = f"""WHERE x."source" = '{m["source"]}' AND {in_window(plan, 'x')}"""
    try:
        view = count(f"SELECT COUNT(*) FROM {name(v, 'view')} x {where}")
    except Exception as error:
        if "does not exist" in str(error):
            return {"status": "na", "note": f"{VIEWS}; the query fails with {message(error)}"}
        raise
    data = {"view": view, "table": count(f"SELECT COUNT(*) FROM {name(m)} x {where}")}
    if data["view"] != data["table"]:
        return {"status": "fail", "data": data, "note": f"The view returns {data['view']} rows, its base table {data['table']}."}
    if data["table"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": data}


def run_row(plan, select, run_id=None):
    """The row of the fixture table with runId = run_id (the run's own by default), and its columns."""
    f = plan["fixture"]
    run_id = run_id or f["run_id"]
    rows, columns = query(f"""SELECT {select} FROM {name(f['table'])} f WHERE f."runId" = '{run_id}'""")
    if not rows:
        raise LookupError(f"{f['table']['table']} has no row with runId = {run_id}.")
    return rows[0], columns


def identifiers(plan):
    e, f = plan["escaped"], plan["fixture"]
    if not query(f"""SELECT x."{e['column']}" FROM {name(e)} x WHERE x."{e['column']}" = '{f['run_id']}'""")[0]:
        return {"status": "fail", "note": f"The escaped table {e['table']} returned no row with {e['column']} = {f['run_id']}."}
    names = f["expect"]["names"]
    row, columns = run_row(plan, ", ".join(f'f."{n}"' for n in names))
    labels = [c.name for c in columns]
    if labels != list(names):
        return {"status": "fail", "note": f"The columns came back as {labels}, not as {list(names)}."}
    wrong = {n: row[i] for i, n in enumerate(names) if row[i] != names[n]}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {"labels": labels}}


def decimal(plan):
    decimals = plan["fixture"]["expect"]["decimals"]
    row, columns = run_row(plan, ", ".join(f'f."{n}"' for n in decimals))
    wrong = {n: f"{row[i]!r} ({kind(columns[i])}({columns[i].precision},{columns[i].scale}))" for i, (n, v) in enumerate(decimals.items())
             if kind(columns[i]) != "FIXED" or not isinstance(row[i], Decimal) or row[i] != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {n: f"{row[i]:f}" for i, n in enumerate(decimals)}}


def timestamps(plan):
    t = plan["fixture"]["expect"]["timestamp"]
    query("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'")  # session-local, writes nothing
    (actual,), (column,) = run_row(plan, f'f."{t["column"]}"')
    data = {"value": str(actual), "type": kind(column)}
    if not isinstance(actual, datetime) or actual.tzinfo is None:
        return {"status": "fail", "data": data, "note": f"Expected an aware datetime, got {actual!r} ({kind(column)})."}
    if actual != datetime.fromisoformat(t["value"]):
        return {"status": "fail", "data": data, "note": f"Expected the instant {t['value']}, got {actual.isoformat()}."}
    if actual.utcoffset() != KOLKATA:
        return {"status": "fail", "data": data, "note": f"The value came back as {actual.isoformat()}, not in the session time zone Asia/Kolkata."}
    return {"status": "ok", "data": data}


def nesting(plan):
    n, lst = plan["fixture"]["expect"]["nested"], plan["fixture"]["expect"]["list"]
    col = f'f."{lst["column"]}"'
    # Path notation on the structured OBJECT, every key quoted so it matches in its exact case.
    leaf = f'f."{n["column"]}":' + ".".join(f'"{p}"' for p in n["path"])
    fields = [(i, k) for i, e in enumerate(lst["value"]) for k in e]
    row, _ = run_row(plan, ", ".join([leaf, f"ARRAY_SIZE({col})", *(f'{col}[{i}]:"{k}"' for i, k in fields)]))
    value, size, items = row[0], row[1], row[2:]
    if value != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {value!r}."}
    want = [Decimal(lst["value"][i][k]) if k == "position" else lst["value"][i][k] for i, k in fields]
    same = size == len(lst["value"]) and all(a == w and (not isinstance(w, Decimal) or isinstance(a, Decimal)) for a, w in zip(items, want))
    if not same:
        got = {f"[{i}].{k}": repr(a) for (i, k), a in zip(fields, items)}
        return {"status": "fail", "note": f"Expected {len(lst['value'])} elements {lst['value']}, got {size}: {got}."}
    return {"status": "ok", "data": {"leaf": value, "elements": size}}


def large_values(plan):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    (text,), _ = run_row(plan, f'f."{large["column"]}"')
    text = text or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, "
                f"got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    rows, _ = query(f"""SELECT t."path", t."data" FROM {name(f['trash'])} t WHERE t."id" = '{f['event']}'""")
    blobs = [d for p, d in rows if (p or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and that path ({len(rows)} rows for the id)."}
    if not isinstance(blobs[0], (bytes, bytearray)) or bytes(blobs[0]).decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(plan):
    e = plan["fixture"]["expect"]["evolution"]
    (run,), _ = run_row(plan, f'f."{e["column"]}"')
    (baseline,), _ = run_row(plan, f'f."{e["column"]}"', e["baseline_run"])
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def incremental_read(plan):
    f = plan["fixture"]
    # Snowflake addresses changes by time, not by Iceberg snapshot id: the appends after the fixture was posted.
    try:
        rows, _ = query(f"""SELECT "runId" FROM {name(f['table'])} CHANGES(INFORMATION => APPEND_ONLY) AT(TIMESTAMP => {ts(f['posted'])})""")
    except Exception as error:
        if re.search(r"(?i)change.?tracking|not supported|unsupported", str(error)):
            return {"status": "na", "note": f"{INCREMENTAL}; the CHANGES query fails with {message(error)}"}
        raise
    got = [r[0] for r in rows]
    if got != [f["run_id"]]:
        return {"status": "fail", "data": {"rows": len(got)}, "note": f"Expected only the run's row, got {len(got)} rows: {got[:3]}."}
    return {"status": "ok", "data": {"rows": 1}}


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


def run(check, plan):
    try:
        return check(plan)
    except Exception as error:  # a check that raises is a genuine failure of the query
        return {"status": "fail", "note": message(error)}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    checks = {check_id: run(check, plan) for check_id, check in CHECKS.items()}
    version = query("SELECT CURRENT_VERSION()")[0][0][0]
    out = {"client": "snowflake", "version": version,
           "build": f"Snowflake {version}, snowflake-connector-python {snowflake.connector.__version__}, catalog-linked database",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2, default=str) + "\n")
    for check_id, result in checks.items():
        print(f"{check_id}: {result['status']}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
