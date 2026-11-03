# /// script
# requires-python = ">=3.11"
# dependencies = ["trino==0.340.0"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with Trino SQL: checks.py <plan.json> <out.json>.

run.sh starts the server with the catalog `lake` (example.properties) and its twin with the case-insensitive-names
switch, so a check that fails plain is run again with the switch. A connect-level switch run.sh needed to start the
server at all comes in TRINO_CONNECT_WORKAROUND, with the plain error in TRINO_CONNECT_ERROR.
"""

import functools
import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import trino  # noqa: E402

from example import connect  # noqa: E402
from harness.redact import redact  # noqa: E402

SWITCHES = ("case-insensitive-names",)  # each a catalog of run.sh's, named lake_<switch>


def catalog(switch=None):
    return "lake" if switch is None else "lake_" + switch.replace("-", "_")


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
    if isinstance(e, trino.exceptions.TrinoQueryError):
        text = f"{e.error_name}: {e.message}"
    else:
        text = f"{type(e).__name__}: {(str(e).strip().splitlines() or [''])[0]}"
    return {"status": "fail", "note": scrub(" ".join(text.split()))[:300]}


def query(catalog_name, sql, **session):
    cursor = connect(catalog_name, **session).cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return [d[0] for d in cursor.description], rows


def count(q, sql):
    return q(sql)[1][0][0]


def name(ref, key="table"):
    return f'"{ref["namespace"]}"."{ref[key]}"'


def ts(iso):
    return f"TIMESTAMP '{iso.replace('T', ' ').removesuffix('Z')} UTC'"


def catalog_login(plan, q):
    listed = {r[0] for r in q("SHOW SCHEMAS")[1]}
    missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
    if missing:
        return {"status": "fail", "note": f"SHOW SCHEMAS does not list {', '.join(missing)}."}
    return {"status": "ok", "data": {"namespaces": len(listed)}}


def s3(plan, q):
    rows = q(f"SELECT * FROM {name(plan['measurement'])} LIMIT 5")[1]
    if not rows:
        return {"status": "fail", "note": "The measurement table returned no rows."}
    return {"status": "ok", "data": {"rows": len(rows)}}


def in_window(plan):
    w = plan["window"]
    return f"time >= {ts(w['from'])} AND time < {ts(w['to'])}"


def cdc_tables(plan, q):
    m, b = plan["measurement"], ts(plan["window"]["boundary"])
    device = f"source = '{m['source']}'"
    where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})", "before": f"time < {b}", "after": f"time >= {b}"}
    data = {k: count(q, f"SELECT count(*) FROM {name(m)} WHERE {in_window(plan)} AND ({v})") for k, v in where.items()}
    if data["device"] + data["not_device"] != data["total"] or data["before"] + data["after"] != data["total"]:
        return {"status": "fail", "data": data, "note": "The filtered counts do not add up to the total."}
    if min(data["device"], data["before"], data["after"]) == 0:
        return {"status": "inconclusive", "data": data, "note": "The window has no rows for the device or on one side of the boundary."}
    return {"status": "ok", "data": data}


def latest_tables(plan, q):
    # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
    rows, ids = q(f"SELECT count(*), count(DISTINCT id) FROM {name(plan['latest'])}")[1][0]
    data = {"rows": rows, "ids": ids}
    if rows == 0:
        return {"status": "inconclusive", "data": data, "note": "The latest table returned no rows."}
    if rows != ids:
        return {"status": "fail", "data": data, "note": f"{rows} rows for {ids} distinct ids, so deleted rows are returned."}
    return {"status": "ok", "data": data}


def views(plan, q):
    v, m = plan["view"], plan["measurement"]
    where = f"WHERE source = '{m['source']}' AND {in_window(plan)}"
    data = {"view": count(q, f"SELECT count(*) FROM {name(v, 'view')} {where}"), "table": count(q, f"SELECT count(*) FROM {name(m)} {where}")}
    if data["view"] != data["table"]:
        return {"status": "fail", "data": data, "note": f"The view returns {data['view']} rows, its base table {data['table']}."}
    if data["table"] == 0:
        return {"status": "inconclusive", "data": data, "note": "The base table has no rows for the device in the window."}
    return {"status": "ok", "data": data}


def run_row(plan, q, select, run_id=None, **session):
    """The row of the fixture table with runId = run_id (the run's own by default), projected to select."""
    f = plan["fixture"]
    run_id = run_id or f["run_id"]
    columns, rows = q(f"SELECT {select} FROM {name(f['table'])} f WHERE f.\"runId\" = '{run_id}'", **session)
    if not rows:
        raise Fail(f"{f['table']['table']} has no row with runId = {run_id}.")
    return columns, rows[0]


def identifiers(plan, q):
    e, f = plan["escaped"], plan["fixture"]
    if not q(f"SELECT \"{e['column']}\" FROM {name(e)} WHERE \"{e['column']}\" = '{f['run_id']}'")[1]:
        return {"status": "fail", "note": f"The escaped table {e['table']} returned no row with {e['column']} = {f['run_id']}."}
    names = f["expect"]["names"]
    labels, row = run_row(plan, q, ", ".join(f'f."{n}"' for n in names))
    if labels != list(names):  # Trino resolves even quoted names case-insensitively; the labels show what came back
        return {"status": "fail", "note": f"The columns came back as {labels}, not as {list(names)}."}
    wrong = {n: row[i] for i, (n, v) in enumerate(names.items()) if row[i] != v}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: names[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok", "data": {"labels": labels}}


def decimal(plan, q):
    decimals = plan["fixture"]["expect"]["decimals"]
    _, row = run_row(plan, q, ", ".join(f'f."{n}"' for n in decimals))
    wrong = {n: repr(row[i]) for i, (n, v) in enumerate(decimals.items()) if not isinstance(row[i], Decimal) or row[i] != Decimal(v)}
    if wrong:
        return {"status": "fail", "note": f"Expected {({n: decimals[n] for n in wrong})}, got {wrong}."}
    return {"status": "ok"}


def timestamps(plan, q):
    t = plan["fixture"]["expect"]["timestamp"]
    _, (actual, zone) = run_row(plan, q, f'f."{t["column"]}", current_timezone()', timezone="Asia/Kolkata")
    if zone != "Asia/Kolkata":
        return {"status": "fail", "note": f"The session time zone is {zone}, not Asia/Kolkata."}
    expected = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
    if not isinstance(actual, datetime) or actual.tzinfo is None or actual != expected:
        return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r}."}
    return {"status": "ok", "data": {"value": actual.isoformat(), "session_time_zone": zone}}


def fields(value):
    """A ROW value as a dict; the Python client returns it as a tuple with the field names in _names."""
    return dict(zip(value._names, value))


def nesting(plan, q):
    n, lst = plan["fixture"]["expect"]["nested"], plan["fixture"]["expect"]["list"]
    leaf = ".".join(f'"{p}"' for p in [n["column"], *n["path"]])
    _, (value, items) = run_row(plan, q, f'f.{leaf}, f."{lst["column"]}"')
    if value != n["value"]:
        return {"status": "fail", "note": f"Expected the leaf {n['value']!r}, got {value!r}."}
    actual = [fields(e) for e in items or []]
    same = len(actual) == len(lst["value"]) and all(
        a.keys() == e.keys() and isinstance(a["position"], Decimal) and a["position"] == Decimal(e["position"])
        and a["label"] == e["label"] for a, e in zip(actual, lst["value"]))
    if not same:
        return {"status": "fail", "note": f"Expected the list {lst['value']}, got {[{k: repr(v) for k, v in a.items()} for a in actual]}."}
    return {"status": "ok"}


def large_values(plan, q):
    f = plan["fixture"]
    large, binned = f["expect"]["large"], f["expect"]["binned"]
    _, (text,) = run_row(plan, q, f'f."{large["column"]}"')
    text = text or ""
    data = {"chars": len(text), "bytes": len(text.encode("utf-8"))}
    if data != {"chars": large["chars"], "bytes": large["bytes"]} or set(text) != {large["char"]}:
        return {"status": "fail", "data": data, "note": f"Expected {large['chars']} chars and {large['bytes']} bytes of {large['char']!r}, "
                f"got {data['chars']} chars, {data['bytes']} bytes of {sorted(set(text))[:5]}."}
    rows = q(f"SELECT path, data FROM {name(f['trash'])} WHERE id = '{f['event']}'")[1]
    blobs = [d for p, d in rows if (p or "").endswith(binned["path_suffix"])]
    if not blobs:
        return {"status": "fail", "data": data, "note": f"The trash table has no row with id = {f['event']} and that path ({len(rows)} rows for the id)."}
    if not isinstance(blobs[0], bytes) or blobs[0].decode("utf-8") != binned["data"]:
        return {"status": "fail", "data": data, "note": f"Expected the binned data {binned['data']!r}, got {blobs[0][:80]!r}."}
    return {"status": "ok", "data": data}


def schema_evolution(plan, q):
    e = plan["fixture"]["expect"]["evolution"]
    _, (run,) = run_row(plan, q, f'f."{e["column"]}"')
    _, (baseline,) = run_row(plan, q, f'f."{e["column"]}"', e["baseline_run"])
    if run != e["run"] or baseline is not None:
        return {"status": "fail", "note": f"Expected {e['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
    return {"status": "ok"}


def incremental_read(plan, q):
    f = plan["fixture"]
    t, posted = f["table"], datetime.fromisoformat(f["posted"])
    snapshots = q(f'SELECT snapshot_id, committed_at, operation FROM "{t["namespace"]}"."{t["table"]}$snapshots" ORDER BY committed_at')[1]
    before = [s for s in snapshots if s[1] < posted]
    after = [s for s in snapshots if s[1] > posted and s[2] == "append"]
    if not before or not after:
        return {"status": "inconclusive", "note": "The table has no snapshot before the fixture was posted, or no append after it."}
    start, end = before[-1][0], after[0][0]
    # table_changes reads the changes after start_snapshot_id up to and including end_snapshot_id.
    rows = q(f"""SELECT "runId", _change_type FROM TABLE(system.table_changes(schema_name => '{t["namespace"]}',
        table_name => '{t["table"]}', start_snapshot_id => {start}, end_snapshot_id => {end}))""")[1]
    data = {"start_snapshot": start, "end_snapshot": end, "rows": len(rows)}
    if [tuple(r) for r in rows] != [(f["run_id"], "insert")]:
        return {"status": "fail", "data": data, "note": f"Expected only the run's row, got {len(rows)} rows: {[tuple(r) for r in rows][:3]}."}
    return {"status": "ok", "data": data}


def attempt(check, plan):
    """The check plain; if it fails, again with each switch, and warn when one of them makes it pass.

    When none does, the result is the failure with the switch if it differs from the plain one, since the plain
    one is then only the failure the switch addresses."""
    plain = last = None
    for switch in (None, *SWITCHES):
        try:
            result = check(plan, functools.partial(query, catalog(switch)))
        except Exception as e:  # a check that raises is a fail with the error
            result = failure(e)
        if switch is None and result["status"] != "fail":
            return result
        if switch and result["status"] == "ok":
            return {**result, "status": "warn", "workaround": switch, "note": f"Without it: {plain.get('note')}"}
        if switch is None:
            plain = last = result
        elif result.get("note") != plain.get("note"):
            last = {**result, "note": f"With the {switch} switch: {result.get('note')}"}
    return last


CHECKS = {"catalog-login": catalog_login, "s3": s3, "cdc-tables": cdc_tables, "latest-tables": latest_tables,
          "identifiers": identifiers, "views": views, "decimal": decimal, "timestamps": timestamps,
          "nesting": nesting, "large-values": large_values, "schema-evolution": schema_evolution,
          "incremental-read": incremental_read}


# The error Trino gives for a view without default catalog, which the patch sketched there fixes.
VIEW_WITHOUT_CATALOG = "Schema must be specified when session schema is not set"
VIEW_FIX = "A fix is sketched in https://github.com/Cumulocity-IoT/trino-fixes/pull/1."


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    checks = {}
    for check_id, check in CHECKS.items():
        checks[check_id] = attempt(check, plan)
        if check_id == "views" and VIEW_WITHOUT_CATALOG in checks[check_id].get("note", ""):
            checks[check_id]["note"] += " " + VIEW_FIX
        print(check_id, checks[check_id]["status"], checks[check_id].get("workaround", ""), file=sys.stderr)
    switch = os.environ.get("TRINO_CONNECT_WORKAROUND")
    if switch and checks["catalog-login"]["status"] in ("ok", "warn"):
        error = scrub(os.environ.get("TRINO_CONNECT_ERROR") or "no error in the server log")[:200]
        checks["catalog-login"] = {**checks["catalog-login"], "status": "warn", "workaround": switch,
                                   "note": f"Without it the catalog does not load and the server does not start: {error}."}
    version = query("lake", "SELECT version()")[1][0][0]
    out = {"client": "trino", "version": version,
           "build": f"Trino {version} (trino-server-core with the trino-iceberg plugin), trino Python client "
                    f"{trino.__version__}, Java {os.environ.get('TRINO_JAVA', 'unknown')}",
           "checks": checks}
    Path(out_path).write_text(json.dumps(out, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
