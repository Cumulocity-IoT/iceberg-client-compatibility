# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql==1.2.3"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with StarRocks SQL: checks.py <plan.json> <out.json>.

run.sh starts the server. The checks register the plain catalog `lake` (example.py); a check that fails there
runs again on `lake_s3_region`, the same catalog with the s3-region switch.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import pymysql  # noqa: E402

from example import connect, create_catalog  # noqa: E402
from harness.redact import redact  # noqa: E402

KOLKATA = timezone(timedelta(hours=5, minutes=30))  # Asia/Kolkata has no daylight saving time

INCREMENTAL = ("StarRocks SQL reads an Iceberg table at one snapshot only (VERSION AS OF, TIMESTAMP AS OF) and has no "
               "clause or table function for the rows appended between two snapshots "
               "(docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel).")
VIEWS = "StarRocks runs the view's starrocks SQL representation."


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


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    connection = connect()
    cursor = connection.cursor()

    def query(sql):
        cursor.execute(sql)
        return [d[0] for d in cursor.description], cursor.fetchall()

    def count(sql):
        return query(sql)[1][0][0]

    # The window literals are UTC; the session zone decides how StarRocks reads them.
    cursor.execute("SET time_zone = 'UTC'")
    w, m, fx = plan["window"], plan["measurement"], plan["fixture"]
    ex = fx["expect"]

    def lit(iso):
        return f"'{iso.replace('T', ' ').rstrip('Z')}'"

    in_window = f"x.`time` >= {lit(w['from'])} AND x.`time` < {lit(w['to'])}"
    device = f"x.source = '{m['source']}'"

    def catalog_login(cat):
        listed = {r[0] for r in query(f"SHOW DATABASES FROM `{cat}`")[1]}
        missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
        if missing:
            return {"status": "fail", "note": f"SHOW DATABASES does not list {', '.join(missing)}."}
        return {"status": "ok", "data": {"namespaces": len(listed)}}

    def s3(cat):
        rows = query(f"SELECT * FROM `{cat}`.`{m['namespace']}`.`{m['table']}` LIMIT 5")[1]
        if not rows:
            return {"status": "fail", "note": "The measurement table returned no rows."}
        return {"status": "ok", "data": {"rows": len(rows)}}

    def cdc_tables(cat):
        table = f"`{cat}`.`{m['namespace']}`.`{m['table']}`"
        b = lit(w["boundary"])
        where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})",
                 "before": f"x.`time` < {b}", "after": f"x.`time` >= {b}"}
        d = {k: count(f"SELECT count(*) FROM {table} x WHERE {in_window} AND ({v})") for k, v in where.items()}
        if d["device"] + d["not_device"] != d["total"] or d["before"] + d["after"] != d["total"]:
            return {"status": "fail", "data": d, "note": "The filtered counts do not add up to the total."}
        if min(d["device"], d["before"], d["after"]) == 0:
            return {"status": "inconclusive", "data": d, "note": "The window has no rows for the device or on one side of the boundary."}
        return {"status": "ok", "data": d}

    def latest_tables(cat):
        table = f"`{cat}`.`{plan['latest']['namespace']}`.`{plan['latest']['table']}`"
        # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
        rows, ids = query(f"SELECT count(*), count(DISTINCT id) FROM {table}")[1][0]
        files = dict(query(f"SELECT content, count(*) FROM `{cat}`.`{plan['latest']['namespace']}`."
                           f"`{plan['latest']['table']}$files` GROUP BY content")[1])
        d = {"rows": rows, "ids": ids, "position_delete_files": files.get(1, 0), "equality_delete_files": files.get(2, 0)}
        if rows == 0:
            return {"status": "inconclusive", "data": d, "note": "The latest table is empty."}
        if rows != ids:
            return {"status": "fail", "data": d, "note": f"{rows} rows for {ids} distinct ids, so deleted rows are returned."}
        return {"status": "ok", "data": d}

    def run_row(cat, cols, run_id=fx["run_id"]):
        t = fx["table"]
        names, rows = query(f"SELECT {cols} FROM `{cat}`.`{t['namespace']}`.`{t['table']}` f WHERE f.`runId` = '{run_id}'")
        if not rows:
            raise Fail(f"{t['namespace']}.{t['table']} returned no row with runId = {run_id}.")
        return names, rows[0]

    def identifiers(cat):
        e = plan["escaped"]
        names, rows = query(f"SELECT x.`{e['column']}` FROM `{cat}`.`{e['namespace']}`.`{e['table']}` x "
                            f"WHERE x.`{e['column']}` = '{fx['run_id']}'")
        if names != [e["column"]]:
            return {"status": "fail", "note": f"The column came back as {names}, not as {e['column']}."}
        if not rows:
            return {"status": "fail", "note": f"The escaped table returned no row with {e['column']} = {fx['run_id']}."}
        expected = ex["names"]
        names, row = run_row(cat, ", ".join(f"f.`{n}`" for n in expected))
        if names != list(expected):
            return {"status": "fail", "note": f"The columns came back as {names}, not as {list(expected)}."}
        wrong = [f"{n}: expected {v!r}, got {row[i]!r}" for i, (n, v) in enumerate(expected.items()) if row[i] != v]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok", "data": {"columns": len(expected)}}

    def decimal(cat):
        decimals = ex["decimals"]
        _, row = run_row(cat, ", ".join(f"f.`{n}`" for n in decimals))
        # pymysql returns DECIMAL columns as Decimal; a float or a string here is a wrong type, not a match.
        wrong = [f"{n}: expected {v}, got {row[i]!r}" for i, (n, v) in enumerate(decimals.items())
                 if not isinstance(row[i], Decimal) or row[i] != Decimal(v)]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok"}

    def timestamps(cat):
        t = ex["timestamp"]
        cursor.execute("SET time_zone = 'Asia/Kolkata'")
        try:
            _, row = run_row(cat, f"f.`{t['column']}`")
        finally:
            cursor.execute("SET time_zone = 'UTC'")
        expected = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
        # StarRocks maps timestamptz to DATETIME, which carries no zone: the value is local time in the session zone.
        if not isinstance(row[0], datetime) or row[0].replace(tzinfo=KOLKATA) != expected:
            return {"status": "fail", "note": f"Expected the instant {t['value']} (Asia/Kolkata "
                    f"{expected.astimezone(KOLKATA):%Y-%m-%d %H:%M:%S.%f}), got {row[0]!r}."}
        return {"status": "ok", "data": {"value": row[0].isoformat()}}

    def nesting(cat):
        n, lst = ex["nested"], ex["list"]
        leaf = "f.`" + "`.`".join([n["column"], *n["path"]]) + "`"
        col = f"f.`{lst['column']}`"
        # Each list element through StarRocks' own subscript and subfield access, so the positions arrive as DECIMAL.
        fields = [f"{col}[{i + 1}].`{k}`" for i, e in enumerate(lst["value"]) for k in e]
        _, row = run_row(cat, ", ".join([leaf, f"array_length({col})", *fields]))
        if row[0] != n["value"]:
            return {"status": "fail", "note": f"The leaf: expected {n['value']!r}, got {row[0]!r}."}
        values = iter(row[2:])
        actual = [{k: next(values) for k in e} for e in lst["value"]]
        same = row[1] == len(lst["value"]) and all(
            isinstance(a["position"], Decimal) and a["position"] == Decimal(e["position"]) and a["label"] == e["label"]
            for a, e in zip(actual, lst["value"]))
        if not same:
            got = [{k: str(v) for k, v in a.items()} for a in actual]
            return {"status": "fail", "note": f"The list: expected {lst['value']}, got {row[1]} elements {got}."}
        return {"status": "ok"}

    def large_values(cat):
        g, b = ex["large"], ex["binned"]
        _, row = run_row(cat, f"f.`{g['column']}`")
        text = row[0] or ""
        got = (len(text), len(text.encode("utf-8")), set(text))
        if got != (g["chars"], g["bytes"], {g["char"]}):
            return {"status": "fail", "note": f"Expected {g['chars']} chars, {g['bytes']} bytes, all {g['char']!r}; "
                    f"got {got[0]} chars, {got[1]} bytes, characters {sorted(got[2])[:5]}."}
        t = fx["trash"]
        rows = query(f"SELECT x.path, x.data FROM `{cat}`.`{t['namespace']}`.`{t['table']}` x WHERE x.id = '{fx['event']}'")[1]
        binned = [data for path, data in rows if (path or "").endswith(b["path_suffix"])]
        if not binned:
            return {"status": "fail", "note": f"The trash table has no row with id = {fx['event']} and that path ({len(rows)} rows for the id)."}
        if not isinstance(binned[0], bytes):
            return {"status": "fail", "note": f"The binary data came back as {type(binned[0]).__name__}, not as bytes."}
        if binned[0].decode("utf-8") != b["data"]:
            return {"status": "fail", "note": f"The binned data: expected {b['data']!r}, got {binned[0][:80]!r}."}
        return {"status": "ok", "data": {"chars": got[0], "bytes": got[1]}}

    def schema_evolution(cat):
        v = ex["evolution"]
        _, run = run_row(cat, f"f.`{v['column']}`")
        _, baseline = run_row(cat, f"f.`{v['column']}`", v["baseline_run"])
        if run[0] != v["run"] or baseline[0] is not None:
            return {"status": "fail", "note": f"Expected {v['run']!r} on the run's row and NULL on the baseline row, "
                    f"got {run[0]!r} and {baseline[0]!r}."}
        return {"status": "ok"}

    def views(cat):
        v = plan["view"]
        sql = "SELECT count(*) FROM {} x WHERE " + f"{device} AND {in_window}"
        d = {"view": count(sql.format(f"`{cat}`.`{v['namespace']}`.`{v['view']}`")),
             "table": count(sql.format(f"`{cat}`.`{m['namespace']}`.`{m['table']}`"))}
        if d["view"] != d["table"]:
            return {"status": "fail", "data": d, "note": f"The view returns {d['view']} rows, its base table {d['table']}."}
        if d["table"] == 0:
            return {"status": "inconclusive", "data": d, "note": "The base table has no rows for the device in the window."}
        return {"status": "ok", "data": d, "note": VIEWS}

    def attempt(check):
        try:
            plain = check("lake")
        except Exception as e:  # a check that raises is a fail with the error
            plain = failure(e)
        if plain["status"] != "fail":
            return plain
        try:
            create_catalog(cursor, "lake_s3_region", **{"aws.s3.region": plan["region"]})
            retried = check("lake_s3_region")
        except Exception:
            return plain
        return {**retried, "status": "warn", "workaround": "s3-region"} if retried["status"] == "ok" else plain

    try:
        create_catalog(cursor)
        login_failed = None
    except Exception as e:  # StarRocks logs in while it registers the catalog, so no check can run
        login_failed = failure(e)
    checks = {}
    for check_id, check in [
        ("catalog-login", catalog_login), ("s3", s3), ("cdc-tables", cdc_tables), ("latest-tables", latest_tables),
        ("identifiers", identifiers), ("views", views), ("decimal", decimal), ("timestamps", timestamps),
        ("nesting", nesting), ("large-values", large_values), ("schema-evolution", schema_evolution),
    ]:
        checks[check_id] = login_failed or attempt(check)
        print(check_id, checks[check_id]["status"], checks[check_id].get("workaround", ""), file=sys.stderr)
    checks["incremental-read"] = {"status": "na", "note": INCREMENTAL}

    version = query("SELECT current_version()")[1][0][0]  # for example 4.1.7-b81ff9c
    out = {"client": "starrocks", "version": version.split("-")[0],
           "build": f"StarRocks {version} (starrocks/allin1-ubuntu image, FE on Iceberg Java's REST catalog), "
                    f"queried over the MySQL protocol with PyMySQL {pymysql.VERSION_STRING}",
           "checks": checks}
    connection.close()
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
