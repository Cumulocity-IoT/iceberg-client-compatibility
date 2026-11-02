# /// script
# requires-python = ">=3.10,<3.14"
# dependencies = ["pyspark==4.1.4"]
# ///
"""Runs the contract checks with Spark SQL: checks.py <plan.json> <out.json>, from the repository root."""

import json
import os
import sys

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

    def identifiers():
        e = plan["escaped"]
        df = spark.sql(f"SELECT `{e['column']}` FROM {name(e['namespace'], e['table'])} LIMIT 5")
        rows = df.collect()
        if df.columns != [e["column"]]:
            return {"status": "fail", "note": f"The column came back as {df.columns}, not as {e['column']}."}
        if not rows:
            # Only an empty table excuses zero rows; a table with a snapshot that reads as empty is a failure.
            snapshots = count(f"SELECT count(*) FROM {name(e['namespace'], e['table'], 'snapshots')}")
            if snapshots == 0:
                return {"status": "inconclusive", "note": "The escaped table has no snapshot, so no rows could come back."}
            return {"status": "fail", "note": "The escaped table has a snapshot, but no rows came back."}
        return {"status": "ok", "data": {"rows": len(rows)}}

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
