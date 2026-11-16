# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3==1.44.4"]
# ///
"""Runs the contract checks with Athena SQL on the federated catalog: checks.py <plan.json> <out.json>.

AWS Glue federates only the tables whose names have no capitals. A check that does not find a table of the plan for
that reason runs again with the lowercase-tables switch, on the plan's lowercase stand-ins (clients/CONTRACT.md).
"""

import copy
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import boto3  # noqa: E402

from example import CATALOG, WORKGROUP, query, session  # noqa: E402
from harness.redact import redact  # noqa: E402

SWITCH = "lowercase-tables"
SOURCE = ("An AWS Glue connection was provisioned with the OAuth client credential and CREDENTIAL_VENDING_MODE = "
          "SOURCE_VENDED, and a federated catalog on it.")
CASING = ("AWS Glue catalog federation leaves out the tables whose names mix lowercase and uppercase letters, "
          "as its connection property CATALOG_CASING_FILTER takes LOWERCASE_ONLY or UPPERCASE_ONLY")
VIEWS = ('AWS Glue catalog federation discovers "Iceberg tables in external catalogs" '
         "(docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation.html) and none of the catalog's views, "
         "not even those with lowercase names such as view_alarm.alarm")
INCREMENTAL = ("Athena SQL reads an Iceberg table at one snapshot (FOR VERSION AS OF, FOR TIMESTAMP AS OF) and has no "
               "clause or table function for the rows appended between two snapshots "
               "(docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html).")
NO_SESSION_ZONE = ("Athena has no session time zone; the API returns the instant as text with its zone, UTC, read here "
                   "as an aware datetime.")
AS_TEXT = "The API returns every value as text with its type; decimal(38,9) is read here as an exact decimal."
INTEGERS = ("tinyint", "smallint", "integer", "bigint")


class Fail(Exception):
    """A check result of fail; the message is the note."""


def message(error):
    if isinstance(error, Fail):
        return redact(str(error))[:300]
    # A failed query raises RuntimeError with Athena's reason, which names the error itself.
    text = str(error) if type(error) is RuntimeError else f"{type(error).__name__}: {error}"
    return redact(" ".join(text.split()))[:300]


def typed(value, kind):
    """Athena's text of a value as the Python value of its type."""
    if value is None:
        return None
    if kind == "decimal":
        return Decimal(value)
    if kind in INTEGERS:
        return int(value)
    if kind in ("double", "real", "float"):
        return float(value)
    if kind == "boolean":
        return value == "true"
    if kind == "varbinary":
        return bytes.fromhex(value)  # hex pairs separated by spaces
    if kind.startswith("timestamp"):
        text, _, zone = value.rpartition(" ") if kind.endswith("with time zone") else (value, "", "")
        instant = datetime.fromisoformat(text)
        return instant.replace(tzinfo=timezone.utc) if zone == "UTC" else instant
    return value


def q(sql):
    """The column labels and the rows of a query, every value as the Python value of its type."""
    columns, rows = query(sql)
    return [name for name, _ in columns], [[typed(v, kind) for v, (_, kind) in zip(row, columns)] for row in rows]


def count(sql):
    return q(sql)[1][0][0]


def name(ref, key="table"):
    return f'"{ref["namespace"]}"."{ref[key]}"'


def ts(iso):
    return f"TIMESTAMP '{iso.replace('T', ' ').removesuffix('Z')} UTC'"


def in_window(plan, alias):
    w = plan["window"]
    return f'{alias}."time" >= {ts(w["from"])} AND {alias}."time" < {ts(w["to"])}'


def catalog_login(plan):
    account = session().client("sts").get_caller_identity()["Account"]
    pages = session().client("glue").get_paginator("get_databases").paginate(CatalogId=f"{account}:{CATALOG}")
    listed = {d["Name"] for page in pages for d in page["DatabaseList"]}
    f = plan["fixture"]
    wanted = {plan[k]["namespace"] for k in ("measurement", "latest", "escaped")} | {f["table"]["namespace"], f["trash"]["namespace"]}
    missing = sorted(wanted - listed)
    if missing:
        return {"status": "fail", "note": f"The federated catalog does not list {', '.join(missing)}."}
    return {"status": "ok", "data": {"namespaces": len(listed)}, "note": SOURCE}


def s3(plan):
    rows = q(f"SELECT * FROM {name(plan['measurement'])} LIMIT 5")[1]
    if not rows:
        return {"status": "fail", "note": "The measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": len(rows)}}


def cdc_tables(plan):
    m, b = plan["measurement"], ts(plan["window"]["boundary"])
    device = f"""m."source" = '{m["source"]}'"""
    where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})", "before": f'm."time" < {b}', "after": f'm."time" >= {b}'}
    data = {k: count(f"SELECT count(*) FROM {name(m)} m WHERE {in_window(plan, 'm')} AND ({v})") for k, v in where.items()}
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(plan):
    # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
    rows, ids = q(f'SELECT count(*), count(DISTINCT "id") FROM {name(plan["latest"])}')[1][0]
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
        view = count(f"SELECT count(*) FROM {name(v, 'view')} x {where}")
    except RuntimeError as error:
        if "TABLE_NOT_FOUND" in str(error):
            return {"status": "na", "note": f"{VIEWS}; the query fails with {message(error)}"}
        raise
    data = {"view": view, "table": count(f"SELECT count(*) FROM {name(m)} x {where}")}
    if data["view"] != data["table"]:
        return {"status": "fail", "data": data, "note": f"The view returns {data['view']} rows, its base table {data['table']}."}
    if data["table"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": data}


def run_row(plan, select, run_id=None):
    """The labels and the row of the fixture table with runId = run_id (the run's own by default), projected to select."""
    f = plan["fixture"]
    run_id = run_id or f["run_id"]
    labels, rows = q(f"""SELECT {select} FROM {name(f['table'])} f WHERE f."runId" = '{run_id}'""")
    if not rows:
        raise Fail(f"{f['table']['table']} has no row with runId = {run_id}.")
    return labels, rows[0]


def identifiers(plan):
    e, f = plan["escaped"], plan["fixture"]
    if not q(f"""SELECT x."{e['column']}" FROM {name(e)} x WHERE x."{e['column']}" = '{f['run_id']}'""")[1]:
        return {"status": "fail", "note": f"The escaped table {e['table']} returned no row with {e['column']} = {f['run_id']}."}
    names = f["expect"]["names"]
    labels, row = run_row(plan, ", ".join(f'f."{n}"' for n in names))
    if labels != list(names):
        return {"status": "fail", "note": f"The columns came back as {labels}, not as {list(names)}."}
    wrong = {n: row[i] for i, n in enumerate(names) if row[i] != names[n]}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {"labels": labels}}


def decimal(plan):
    decimals = plan["fixture"]["expect"]["decimals"]
    _, row = run_row(plan, ", ".join(f'f."{n}"' for n in decimals))
    wrong = {n: repr(row[i]) for i, (n, v) in enumerate(decimals.items()) if not isinstance(row[i], Decimal) or row[i] != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {n: f"{row[i]:f}" for i, n in enumerate(decimals)}, "note": AS_TEXT}


def timestamps(plan):
    t = plan["fixture"]["expect"]["timestamp"]
    os.environ["TZ"] = "Asia/Kolkata"  # the process time zone; Athena has no session time zone to set
    time.tzset()
    _, (actual,) = run_row(plan, f'f."{t["column"]}"')
    expected = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
    if not isinstance(actual, datetime) or actual.tzinfo is None or actual != expected:
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r}."}
    return {"status": "ok", "data": {"value": actual.isoformat()}, "note": NO_SESSION_ZONE}


def nesting(plan):
    n, lst = plan["fixture"]["expect"]["nested"], plan["fixture"]["expect"]["list"]
    leaf = "f." + ".".join(f'"{p}"' for p in [n["column"], *n["path"]])
    col = f'f."{lst["column"]}"'
    fields = [(i, k) for i, e in enumerate(lst["value"]) for k in e]
    # element_at counts from 1 and gives NULL past the end, where a subscript would fail the query.
    _, row = run_row(plan, ", ".join([leaf, f"cardinality({col})", *(f'element_at({col}, {i + 1})."{k}"' for i, k in fields)]))
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
    _, (text,) = run_row(plan, f'f."{large["column"]}"')
    text = text or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, "
                f"got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    rows = q(f"""SELECT t."path", t."data" FROM {name(f['trash'])} t WHERE t."id" = '{f['event']}'""")[1]
    blobs = [d for p, d in rows if (p or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and that path ({len(rows)} rows for the id)."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(plan):
    e = plan["fixture"]["expect"]["evolution"]
    _, (run,) = run_row(plan, f'f."{e["column"]}"')
    _, (baseline,) = run_row(plan, f'f."{e["column"]}"', e["baseline_run"])
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def incremental_read(plan):
    return {"status": "na", "note": INCREMENTAL}


def lowercase(plan):
    """The plan with its lowercase stand-ins for the measurement table and the fixture table (lowercase-tables)."""
    p = copy.deepcopy(plan)
    p["measurement"] = plan["lowercase"]
    stand_in = plan["fixture"]["lowercase"]
    p["fixture"]["table"] = {"namespace": stand_in["namespace"], "table": stand_in["table"]}
    p["fixture"]["expect"]["evolution"]["baseline_run"] = stand_in["baseline_run"]
    return p


def run(check, plan):
    try:
        return check(plan)
    except Exception as error:  # a check that raises is a fail with the error
        if "TABLE_NOT_FOUND" in str(error):
            return {"status": "fail", "note": f"{CASING}: {message(error)}"}
        return {"status": "fail", "note": message(error)}


def attempt(check, plan, switchable):
    """The check plain; if a table was left out for its name, again with the switch, and warn when that passes."""
    plain = run(check, plan)
    if not switchable or plain["status"] != "fail" or not plain["note"].startswith(CASING):
        return plain
    switched = run(check, lowercase(plan))
    if switched["status"] == "ok":
        return {**switched, "status": "warn", "workaround": SWITCH,
                "note": f"The check reads the same data from tables whose names have no capitals. Without it: {plain['note']}"}
    if switched.get("note") != plain.get("note"):
        return {**switched, "note": f"With the {SWITCH} switch: {switched.get('note')}"}
    return plain


# Checks on a table the plan names with capitals, which have a lowercase stand-in; the identifiers check is about
# the exact names, so it has none.
SWITCHABLE = {"s3", "cdc-tables", "decimal", "timestamps", "nesting", "large-values", "schema-evolution"}
CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    checks = {}
    for check_id, check in CHECKS.items():
        checks[check_id] = attempt(check, plan, check_id in SWITCHABLE)
        print(check_id, checks[check_id]["status"], checks[check_id].get("workaround", ""), file=sys.stderr)
    engine = session().client("athena").get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]["Configuration"]["EngineVersion"]
    effective = engine["EffectiveEngineVersion"]  # "Athena engine version 3"
    version = re.search(r"(\d+)\s*$", effective).group(1)
    out = {"client": "athena", "version": version,
           "build": f"{effective} (workgroup {WORKGROUP}), AWS Glue catalog federation with SOURCE_VENDED, boto3 {boto3.__version__}",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
