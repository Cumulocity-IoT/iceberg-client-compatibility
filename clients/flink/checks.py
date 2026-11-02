# /// script
# requires-python = ">=3.10,<3.13"
# dependencies = ["apache-flink==2.3.0"]
# [tool.uv]
# build-constraint-dependencies = ["setuptools<81"]  # apache-beam builds from source on macOS arm64 and needs pkg_resources
# ///
"""Runs the contract checks with Flink SQL in batch mode: checks.py <plan.json> <out.json>, from the repository root."""

import json
import math
import os
import re
import sys
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)), os.getcwd()]

from example import FLINK_MINOR, HADOOP, ICEBERG, table_env  # noqa: E402
from harness.redact import redact  # noqa: E402
from pyflink.java_gateway import get_gateway  # noqa: E402


def scrub(text):
    secret = os.environ.get("CATALOG_CREDENTIAL", "")
    for value in [secret, *secret.split(":")]:
        if len(value) >= 4:
            text = text.replace(value, "REDACTED")
    return redact(text)


def error_note(e):
    """The Java exception and its root cause, without the Py4J wrapper and the stack trace."""
    lines = [s for s in (line.strip().lstrip(": ") for line in str(e).splitlines()) if s and not s.startswith(("at ", "..."))]
    head = next((s for s in lines if "Exception" in s or "Error" in s), lines[0] if lines else type(e).__name__)
    cause = next((s for s in reversed(lines) if s.startswith("Caused by:")), "")
    return scrub(" ".join(f"{head} {cause}".split())[:400])


def name(*parts):
    return "lake." + ".".join(f"`{p}`" for p in parts)


def millis(iso):
    return (datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def ltz(iso):
    """An instant literal, independent of the session time zone (TIMESTAMP '...' would be read in it)."""
    return f"TO_TIMESTAMP_LTZ({round(millis(iso))}, 3)"


def main(plan_path, out_path):
    with open(plan_path) as f:
        plan = json.load(f)
    try:
        env = table_env()
    except Exception as e:  # a failed CREATE CATALOG can echo its options, the credential among them
        write(out_path, {"catalog-login": {"status": "fail", "note": error_note(e)}}, "unknown")
        return

    def rows(sql):
        with env.execute_sql(sql).collect() as result:
            return list(result)

    def count(sql):
        return rows(sql)[0][0]

    w, m = plan["window"], plan["measurement"]
    measurement = name(m["namespace"], m["table"])
    in_window = f"`time` >= {ltz(w['from'])} AND `time` < {ltz(w['to'])}"
    device = f"`source` = '{m['source']}'"

    def catalog_login():
        listed = {r[0] for r in rows("SHOW DATABASES IN lake")}
        wanted = {plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")}
        missing = sorted(wanted - listed)
        if missing:
            return {"status": "fail", "note": f"Namespaces missing from the listing: {', '.join(missing)}."}
        return {"status": "ok", "data": {"namespaces": len(listed)}}

    def s3():
        # Not SELECT *: PyFlink cannot collect the TIMESTAMP_LTZ columns, see timestamps.
        got = rows(f"SELECT `source` FROM {measurement} LIMIT 5")
        if not got:
            return {"status": "fail", "note": "The measurement table returned no rows."}
        return {"status": "ok", "data": {"rows": len(got)}}

    def cdc_tables():
        b = ltz(w["boundary"])
        where = {
            "total": "TRUE",
            "device": device,
            "not_device": f"NOT ({device})",
            "before": f"`time` < {b}",
            "after": f"`time` >= {b}",
        }
        d = {k: count(f"SELECT count(*) FROM {measurement} WHERE {in_window} AND ({v})") for k, v in where.items()}
        if d["device"] + d["not_device"] != d["total"] or d["before"] + d["after"] != d["total"]:
            return {"status": "fail", "data": d, "note": "The filtered counts do not add up to the total."}
        if min(d["device"], d["before"], d["after"]) == 0:
            return {"status": "inconclusive", "data": d, "note": "The window has no rows for the device or on one side of the boundary."}
        return {"status": "ok", "data": d}

    def latest_tables():
        latest = name(plan["latest"]["namespace"], plan["latest"]["table"])
        # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
        row = rows(f"SELECT count(*), count(DISTINCT `id`) FROM {latest}")[0]
        d = {"rows": row[0], "ids": row[1]}
        if d["rows"] == 0:
            return {"status": "inconclusive", "data": d, "note": "The latest table is empty."}
        if d["rows"] != d["ids"]:
            return {"status": "fail", "data": d, "note": f"{d['rows']} rows for {d['ids']} distinct ids, so deleted rows are returned."}
        return {"status": "ok", "data": d}

    fx = plan["fixture"]
    ex = fx["expect"]
    fixture = name(fx["table"]["namespace"], fx["table"]["table"])

    def run_row(cols, run_id=fx["run_id"]):
        table = env.sql_query(f"SELECT {cols} FROM {fixture} WHERE `runId` = '{run_id}'")
        with table.execute().collect() as result:
            got = list(result)
        if not got:
            raise LookupError(f"{fixture} returned no row with runId = {run_id}.")
        return table.get_resolved_schema(), got[0]

    def column_names(schema):
        return list(schema.get_column_names())  # a Py4J JavaList, which never equals a Python list

    def exact(actual, expected):
        # A decimal passes only as a Decimal of the same value; a float or a string does not.
        return isinstance(actual, Decimal) and actual == Decimal(expected)

    def identifiers():
        e = plan["escaped"]
        table = env.sql_query(f"SELECT `{e['column']}` FROM {name(e['namespace'], e['table'])} WHERE `{e['column']}` = '{fx['run_id']}'")
        columns = column_names(table.get_resolved_schema())
        if columns != [e["column"]]:
            return {"status": "fail", "note": f"The column came back as {columns}, not as {e['column']}."}
        with table.execute().collect() as result:
            if not list(result):
                return {"status": "fail", "note": f"The escaped table returned no row with {e['column']} = {fx['run_id']}."}
        names = ex["names"]
        schema, row = run_row(", ".join(f"`{n}`" for n in names))
        if column_names(schema) != list(names):
            return {"status": "fail", "note": f"The columns came back as {column_names(schema)}, not as {list(names)}."}
        wrong = [f"{n}: expected {v!r}, got {row[i]!r}" for i, (n, v) in enumerate(names.items()) if row[i] != v]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok", "data": {"columns": len(names)}}

    def decimal():
        decimals = ex["decimals"]
        _, row = run_row(", ".join(f"`{n}`" for n in decimals))
        wrong = [f"{n}: expected {v}, got {row[i]!r}" for i, (n, v) in enumerate(decimals.items()) if not exact(row[i], v)]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok"}

    def timestamps():
        t = ex["timestamp"]
        env.get_config().set("table.local-time-zone", "Asia/Kolkata")
        expected = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
        try:
            schema, row = run_row(f"`{t['column']}`")
        except Exception as e:  # PyFlink collects TIMESTAMP_LTZ through a pickler that cannot serialize Instant
            if "PickleException" not in str(e):
                raise
            frame = env.sql_query(f"SELECT `{t['column']}` FROM {fixture} WHERE `runId` = '{fx['run_id']}'").to_pandas()
            value = frame.iloc[0, 0] if len(frame) else None
            shown = value.isoformat(timespec="milliseconds") if value is not None else None
            if value is not None and value.tzinfo is not None and value.to_pydatetime() == expected:
                return {"status": "ok", "note": "collect() raises a PickleException on TIMESTAMP_LTZ values (FLINK-38418); "
                        "to_pandas() returns the instant."}
            return {"status": "fail", "note": "collect() raises a PickleException on TIMESTAMP_LTZ values (FLINK-38418), "
                    f"and to_pandas() returns {shown} for the instant {t['value']}."}
        actual = row[0]
        if not isinstance(actual, datetime) or actual.tzinfo is None or actual != expected:
            return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual!r} ({schema.get_column_data_types()[0]})."}
        return {"status": "ok"}

    def nesting():
        n, lst = ex["nested"], ex["list"]
        schema, row = run_row(f"`{n['column']}`." + ".".join(f"`{p}`" for p in n["path"]) + f", `{lst['column']}`")
        if row[0] != n["value"]:
            return {"status": "fail", "note": f"The leaf: expected {n['value']!r}, got {row[0]!r}."}
        # Collected nested rows carry no field names; the result's element type has them.
        fields = schema.get_column_data_types()[1].element_type.field_names()
        actual = [dict(zip(fields, e)) for e in row[1]] if row[1] is not None else None
        if actual is None or len(actual) != len(lst["value"]) or not all(
            a.keys() == e.keys() and all(exact(a[k], v) if k == "position" else a[k] == v for k, v in e.items()) for a, e in zip(actual, lst["value"])
        ):
            return {"status": "fail", "note": f"The list: expected {lst['value']}, got {actual}."}
        return {"status": "ok"}

    def large_values():
        g, b = ex["large"], ex["binned"]
        _, row = run_row(f"`{g['column']}`")
        text = row[0] or ""
        got = (len(text), len(text.encode("utf-8")), set(text))
        if got != (g["chars"], g["bytes"], {g["char"]}):
            return {"status": "fail", "note": f"Expected {g['chars']} chars, {g['bytes']} bytes, all {g['char']!r}; "
                    f"got {got[0]} chars, {got[1]} bytes, characters {sorted(got[2])[:5]}."}
        t = fx["trash"]
        found = rows(f"SELECT `path`, `data` FROM {name(t['namespace'], t['table'])} WHERE `id` = '{fx['event']}'")
        binned = [r[1] for r in found if (r[0] or "").endswith(b["path_suffix"])]
        if not binned:
            return {"status": "fail", "note": f"The trash table has no row with id = {fx['event']} and that path ({len(found)} rows for the id)."}
        if not isinstance(binned[0], (bytes, bytearray)):
            return {"status": "fail", "note": f"The binary data came back as {type(binned[0]).__name__}, not as bytes."}
        data = bytes(binned[0]).decode("utf-8")
        if data != b["data"]:
            return {"status": "fail", "note": f"The binned data: expected {b['data']!r}, got {data[:80]!r}."}
        return {"status": "ok", "data": {"chars": got[0], "bytes": got[1]}}

    def schema_evolution():
        v = ex["evolution"]
        _, run = run_row(f"`{v['column']}`")
        _, baseline = run_row(f"`{v['column']}`", v["baseline_run"])
        if run[0] != v["run"] or baseline[0] is not None:
            return {"status": "fail", "note": f"Expected {v['run']!r} on the run's row and NULL on the baseline row, "
                    f"got {run[0]!r} and {baseline[0]!r}."}
        return {"status": "ok"}

    def incremental_read():
        snapshots = name(fx["table"]["namespace"], fx["table"]["table"] + "$snapshots")
        # Snapshot times are milliseconds: committed before posted exactly when earlier than posted rounded up.
        posted = f"TO_TIMESTAMP_LTZ({math.ceil(millis(fx['posted']))}, 3)"
        before = rows(f"SELECT snapshot_id FROM {snapshots} WHERE committed_at < {posted} ORDER BY committed_at DESC LIMIT 1")
        after = rows(f"SELECT snapshot_id FROM {snapshots} WHERE committed_at >= {posted} AND operation = 'append' "
                     "ORDER BY committed_at LIMIT 1")
        if not before or not after:
            return {"status": "inconclusive", "note": "The fixture table has no snapshot before the fixture was posted, or no append after it."}
        d = {"start": before[0][0], "end": after[0][0]}
        got = [r[0] for r in rows(f"SELECT `runId` FROM {fixture} /*+ OPTIONS('start-snapshot-id'='{d['start']}', "
                                  f"'end-snapshot-id'='{d['end']}') */")]
        d["rows"] = len(got)
        if got != [fx["run_id"]]:
            return {"status": "fail", "data": d, "note": f"Expected only the run's row, got {len(got)} rows with runId {sorted(set(map(str, got)))[:3]}."}
        return {"status": "ok", "data": d}

    def views():
        v = plan["view"]
        sql = "SELECT count(*) FROM {} WHERE " + f"{device} AND {in_window}"
        try:
            d = {"view": count(sql.format(name(v["namespace"], v["view"])))}
        except Exception as e:  # FlinkCatalog#toCatalogView rejects views it would resolve differently than written
            refusal = re.search(r"UnsupportedOperationException: (Cannot read view .*?)$", str(e), re.M)
            if not refusal:
                raise
            # fail, not na: Flink reads Iceberg views; it refuses this one for how it is written.
            return {"status": "fail", "note": f"Iceberg's Flink catalog refuses the view: {scrub(refusal.group(1))}."}
        d["table"] = count(sql.format(measurement))
        if d["view"] != d["table"]:
            return {"status": "fail", "data": d, "note": f"The view returns {d['view']} rows, its base table {d['table']}."}
        if d["table"] == 0:
            return {"status": "inconclusive", "data": d, "note": "The base table has no rows for the device in the window."}
        return {"status": "ok", "data": d}

    checks = {}
    for check_id, check in [
        ("catalog-login", catalog_login),
        ("s3", s3),
        ("cdc-tables", cdc_tables),
        ("latest-tables", latest_tables),
        ("identifiers", identifiers),
        ("views", views),
        ("decimal", decimal),
        ("nesting", nesting),
        ("large-values", large_values),
        ("schema-evolution", schema_evolution),
        ("incremental-read", incremental_read),
        ("timestamps", timestamps),  # last: it changes the session time zone
    ]:
        try:
            checks[check_id] = check()
        except Exception as e:  # a failing check must not stop the others
            checks[check_id] = {"status": "fail", "note": error_note(e)}
        print(check_id, checks[check_id]["status"], file=sys.stderr)

    write(out_path, checks, get_gateway().jvm.java.lang.System.getProperty("java.version"))


def write(out_path, checks, java):
    flink = version("apache-flink")
    out = {
        "client": "flink",
        "version": flink,
        "build": f"apache-flink {flink}, iceberg-flink-runtime-{FLINK_MINOR} {ICEBERG}, iceberg-aws-bundle {ICEBERG}, "
        f"hadoop-client {HADOOP}, Java {java}",
        "checks": checks,
    }
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
