# /// script
# requires-python = ">=3.10,<3.14"
# dependencies = ["pyspark==4.1.4"]
# ///
"""Runs the contract checks with Spark SQL: checks.py <plan.json> <out.json>, from the repository root."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)), os.getcwd()]

import pyspark  # noqa: E402
from example import ICEBERG, spark_session  # noqa: E402
from harness.redact import redact  # noqa: E402


def scrub(text):
    secret = os.environ.get("CATALOG_CREDENTIAL", "")
    for value in [secret, *secret.split(":")]:
        if len(value) >= 4:
            text = text.replace(value, "REDACTED")
    return redact(text)


def error_note(e):
    return scrub(" ".join(str(e).split())[:400])


def name(*parts):
    return "c8y." + ".".join(f"`{p}`" for p in parts)


def main(plan_path, out_path):
    with open(plan_path) as f:
        plan = json.load(f)
    spark = spark_session({"spark.ui.enabled": "false"})
    spark.sparkContext.setLogLevel("ERROR")

    def count(sql):
        return spark.sql(sql).collect()[0][0]

    w, m = plan["window"], plan["measurement"]
    measurement = name(m["namespace"], m["table"])
    in_window = f"time >= TIMESTAMP '{w['from']}' AND time < TIMESTAMP '{w['to']}'"
    device = f"source = '{m['source']}'"

    def catalog_login():
        listed = {r[0] for r in spark.sql("SHOW NAMESPACES IN c8y").collect()}
        wanted = {plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")}
        missing = sorted(wanted - listed)
        if missing:
            return {"status": "fail", "note": f"Namespaces missing from the listing: {', '.join(missing)}."}
        return {"status": "ok", "data": {"namespaces": len(listed)}}

    def s3():
        rows = spark.sql(f"SELECT * FROM {measurement} LIMIT 5").collect()
        if not rows:
            return {"status": "fail", "note": "The measurement table returned no rows."}
        return {"status": "ok", "data": {"rows": len(rows)}}

    def cdc_tables():
        b = f"TIMESTAMP '{w['boundary']}'"
        where = {
            "total": "TRUE",
            "device": device,
            "not_device": f"NOT ({device})",
            "before": f"time < {b}",
            "after": f"time >= {b}",
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
        row = spark.sql(f"SELECT count(*), count(DISTINCT id) FROM {latest}").collect()[0]
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
        df = spark.sql(f"SELECT {cols} FROM {fixture} WHERE runId = '{run_id}'")
        rows = df.collect()
        if not rows:
            raise LookupError(f"{fixture} returned no row with runId = {run_id}.")
        return df.columns, rows[0]

    def same(actual, expected):
        # Exact decimal comparison: Decimal(float) converts a float exactly, never through a short string.
        if isinstance(actual, (Decimal, float)):
            return expected is not None and Decimal(actual) == Decimal(expected)
        return actual == expected

    def identifiers():
        e = plan["escaped"]
        df = spark.sql(f"SELECT `{e['column']}` FROM {name(e['namespace'], e['table'])} WHERE `{e['column']}` = '{fx['run_id']}'")
        if df.columns != [e["column"]]:
            return {"status": "fail", "note": f"The column came back as {df.columns}, not as {e['column']}."}
        if not df.collect():
            return {"status": "fail", "note": f"The escaped table returned no row with {e['column']} = {fx['run_id']}."}
        names = ex["names"]
        columns, row = run_row(", ".join(f"`{n}`" for n in names))
        if columns != list(names):
            return {"status": "fail", "note": f"The columns came back as {columns}, not as {list(names)}."}
        wrong = [f"{n}: expected {v!r}, got {row[i]!r}" for i, (n, v) in enumerate(names.items()) if row[i] != v]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok", "data": {"columns": len(names)}}

    def decimal():
        decimals = ex["decimals"]
        _, row = run_row(", ".join(f"`{n}`" for n in decimals))
        wrong = [f"{n}: expected {v}, got {row[i]!r}" for i, (n, v) in enumerate(decimals.items()) if not same(row[i], v)]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok"}

    def timestamps():
        t = ex["timestamp"]
        spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
        _, row = run_row(f"unix_millis(`{t['column']}`)")
        instant = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
        expected = (instant - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
        if row[0] != expected:
            actual = datetime.fromtimestamp(row[0] / 1000, timezone.utc).isoformat() if row[0] is not None else None
            return {"status": "fail", "note": f"Expected the instant {t['value']}, got {actual}."}
        return {"status": "ok"}

    def nesting():
        n, lst = ex["nested"], ex["list"]
        _, row = run_row(f"`{n['column']}`." + ".".join(f"`{p}`" for p in n["path"]) + f", `{lst['column']}`")
        if row[0] != n["value"]:
            return {"status": "fail", "note": f"The leaf: expected {n['value']!r}, got {row[0]!r}."}
        actual = [e.asDict() for e in row[1]] if row[1] is not None else None
        if actual is None or len(actual) != len(lst["value"]) or not all(
            a.keys() == e.keys() and all(same(a[k], v) for k, v in e.items()) for a, e in zip(actual, lst["value"])
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
        rows = spark.sql(f"SELECT path, data FROM {name(t['namespace'], t['table'])} WHERE id = '{fx['event']}'").collect()
        binned = [r["data"] for r in rows if (r["path"] or "").endswith(b["path_suffix"])]
        if not binned:
            return {"status": "fail", "note": f"The trash table has no row with id = {fx['event']} and that path ({len(rows)} rows for the id)."}
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

    def views():
        v = plan["view"]
        sql = "SELECT count(*) FROM {} WHERE " + f"{device} AND {in_window}"
        d = {"view": count(sql.format(name(v["namespace"], v["view"]))), "table": count(sql.format(measurement))}
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
        ("timestamps", timestamps),
        ("nesting", nesting),
        ("large-values", large_values),
        ("schema-evolution", schema_evolution),
    ]:
        try:
            checks[check_id] = check()
        except Exception as e:  # a failing check must not stop the others
            checks[check_id] = {"status": "fail", "note": error_note(e)}
        print(check_id, checks[check_id]["status"], file=sys.stderr)

    java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    out = {
        "client": "spark",
        "version": pyspark.__version__,
        "build": f"pyspark {pyspark.__version__}, iceberg-spark-runtime-4.1_2.13 {ICEBERG}, "
        f"iceberg-aws-bundle {ICEBERG}, Java {java}",
        "checks": checks,
    }
    spark.stop()
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
