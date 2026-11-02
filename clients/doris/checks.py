# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql==1.2.3"]
# ///
"""Runs the contract checks (clients/CONTRACT.md) with Apache Doris over the MySQL protocol: checks.py <plan.json> <out.json>."""

import json
import os
import re
import sys
from importlib import metadata
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # example.py, and harness.redact from the repository root

import pymysql  # noqa: E402
from pymysql.constants import FIELD_TYPE  # noqa: E402

from example import connect  # noqa: E402
from harness.redact import redact  # noqa: E402

INCREMENTAL_DOCS = ("Doris documents @incr incremental reads for Hudi and Paimon tables, not for Iceberg tables "
                    "(doris.apache.org/docs/lakehouse/catalogs/iceberg-catalog)")


class Fail(Exception):
    """A check result of fail; the message is the note."""


def scrub(text):
    secret = os.environ.get("CATALOG_CREDENTIAL", "")
    for value in [secret, *secret.split(":")]:
        if len(value) >= 4:
            text = text.replace(value, "REDACTED")
    return redact(text)


def name(*parts):
    return "c8y." + ".".join(f"`{p}`" for p in parts)


def main(plan_path, out_path):
    plan = json.loads(Path(plan_path).read_text())
    conn = connect()

    def query(sql):
        """The rows as dicts, and the MySQL field type of each column."""
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = cursor.description or []
            return [dict(zip([c[0] for c in columns], row)) for row in cursor.fetchall()], {c[0]: c[1] for c in columns}

    def count(sql):
        return query(sql)[0][0]["n"]

    server = query("SELECT @@version_comment AS v")[0][0]["v"]  # "Doris version doris-<version>-release-<commit>"
    version = re.search(r"doris-([0-9.]+)", server).group(1)

    w, m = plan["window"], plan["measurement"]
    measurement = name(m["namespace"], m["table"])
    # Literals carry their offset (the plan's ISO instants), so no count depends on the session time zone.
    in_window = f"`time` >= '{w['from']}' AND `time` < '{w['to']}'"
    device = f"`source` = '{m['source']}'"

    def catalog_login():
        listed = {r["Database"] for r in query("SHOW DATABASES FROM c8y")[0]}
        missing = sorted({plan[k]["namespace"] for k in ("measurement", "latest", "escaped", "view")} - listed)
        if missing:
            return {"status": "fail", "note": f"SHOW DATABASES does not list {', '.join(missing)}."}
        return {"status": "ok", "data": {"namespaces": len(listed)}}

    def s3():
        rows = query(f"SELECT * FROM {measurement} LIMIT 5")[0]
        if not rows:
            return {"status": "fail", "note": "The measurement table returned no rows."}
        return {"status": "ok", "data": {"rows": len(rows)}}

    def cdc_tables():
        b = f"'{w['boundary']}'"
        where = {"total": "TRUE", "device": device, "not_device": f"NOT ({device})",
                 "before": f"`time` < {b}", "after": f"`time` >= {b}"}
        d = {k: count(f"SELECT COUNT(*) AS n FROM {measurement} WHERE {in_window} AND ({v})") for k, v in where.items()}
        if d["device"] + d["not_device"] != d["total"] or d["before"] + d["after"] != d["total"]:
            return {"status": "fail", "data": d, "note": "The filtered counts do not add up to the total."}
        if min(d["device"], d["before"], d["after"]) == 0:
            return {"status": "inconclusive", "data": d, "note": "The window has no rows for the device or on one side of the boundary."}
        return {"status": "ok", "data": d}

    def latest_tables():
        latest = name(plan["latest"]["namespace"], plan["latest"]["table"])
        # One query reads one snapshot; two could straddle an upsert of this continuously updated table.
        row = query(f"SELECT COUNT(*) AS n, COUNT(DISTINCT `id`) AS ids FROM {latest}")[0][0]
        # The equality delete files of the current snapshot, which the counts above had to apply.
        d = {"rows": row["n"], "ids": row["ids"],
             "equality_delete_files": count(f"SELECT COUNT(*) AS n FROM {name(plan['latest']['namespace'], plan['latest']['table'] + '$files')} WHERE `content` = 2")}
        if d["rows"] == 0:
            return {"status": "inconclusive", "data": d, "note": "The latest table is empty."}
        if d["rows"] != d["ids"]:
            return {"status": "fail", "data": d, "note": f"{d['rows']} rows for {d['ids']} distinct ids, so deleted rows are returned."}
        return {"status": "ok", "data": d}

    fx = plan["fixture"]
    ex = fx["expect"]
    fixture = name(fx["table"]["namespace"], fx["table"]["table"])

    def run_row(columns, run_id=fx["run_id"]):
        rows, types = query(f"SELECT {columns} FROM {fixture} f WHERE f.`runId` = '{run_id}'")
        if not rows:
            raise Fail(f"{fixture} returned no row with runId = {run_id}.")
        return rows[0], types

    def identifiers():
        e = plan["escaped"]
        rows, types = query(f"SELECT `{e['column']}` FROM {name(e['namespace'], e['table'])} WHERE `{e['column']}` = '{fx['run_id']}'")
        if not rows:
            return {"status": "fail", "note": f"The escaped table returned no row with {e['column']} = {fx['run_id']}."}
        names = ex["names"]
        row, types = run_row(", ".join(f"`{n}`" for n in names))
        if list(types) != list(names):
            return {"status": "fail", "note": f"The columns came back as {list(types)}, not as {list(names)}."}
        wrong = [f"{n}: expected {v!r}, got {row[n]!r}" for n, v in names.items() if row[n] != v]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok", "data": {"columns": len(names)}}

    def decimal():
        decimals = ex["decimals"]
        row, types = run_row(", ".join(f"`{n}`" for n in decimals))
        wrong = [f"{n}: expected {v}, got {row[n]!r} (MySQL type {types[n]})" for n, v in decimals.items()
                 if types[n] != FIELD_TYPE.NEWDECIMAL or not isinstance(row[n], Decimal) or row[n] != Decimal(v)]
        if wrong:
            return {"status": "fail", "note": "; ".join(wrong) + "."}
        return {"status": "ok", "data": {n: format(row[n], "f") for n in decimals}}

    def timestamps():
        t = ex["timestamp"]
        query("SET time_zone = 'Asia/Kolkata'")
        row, _ = run_row(f"`{t['column']}` AS text, MILLISECOND_TIMESTAMP(`{t['column']}`) AS ms")
        instant = datetime.fromisoformat(t["value"].replace("Z", "+00:00"))
        expected = (instant - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
        d = {"text": row["text"], "ms": row["ms"]}
        if not isinstance(row["ms"], int) or row["ms"] != expected:
            return {"status": "fail", "data": d, "note": f"Expected the instant {t['value']} ({expected} ms), got {row['ms']!r} ms."}
        shown = datetime.fromisoformat(row["text"])
        if shown != instant or shown.utcoffset() != timedelta(hours=5, minutes=30):
            return {"status": "fail", "data": d, "note": f"Expected {t['value']} shown at +05:30, got {row['text']!r}."}
        return {"status": "ok", "data": d,
                "note": "The MySQL protocol carries TIMESTAMPTZ(6) as text in the session time zone, so the instant is "
                        "read as epoch milliseconds through MILLISECOND_TIMESTAMP."}

    def nesting():
        n, lst = ex["nested"], ex["list"]
        leaf = ".".join(f"`{p}`" for p in [n["column"], *n["path"]])
        # Typed values through Doris' own element access; the whole list would come back as one JSON-like string.
        fields = [f"STRUCT_ELEMENT(f.`{lst['column']}`[{i}], '{k}') AS `{i}.{k}`"
                  for i, e in enumerate(lst["value"], 1) for k in e]
        row, _ = run_row(", ".join([f"f.{leaf} AS leaf", f"SIZE(f.`{lst['column']}`) AS size", *fields]))
        if row["leaf"] != n["value"]:
            return {"status": "fail", "note": f"The leaf: expected {n['value']!r}, got {row['leaf']!r}."}
        actual = [{k: row[f"{i}.{k}"] for k in e} for i, e in enumerate(lst["value"], 1)]
        same = row["size"] == len(lst["value"]) and all(
            isinstance(a["position"], Decimal) and a["position"] == Decimal(e["position"]) and a["label"] == e["label"]
            for a, e in zip(actual, lst["value"]))
        if not same:
            got = [{k: str(v) for k, v in a.items()} for a in actual]
            return {"status": "fail", "note": f"The list: expected {lst['value']}, got {row['size']} elements starting {got}."}
        return {"status": "ok"}

    def large_values():
        g, b = ex["large"], ex["binned"]
        text = run_row(f"`{g['column']}`")[0][g["column"]] or ""
        got = (len(text), len(text.encode("utf-8")), set(text))
        if got != (g["chars"], g["bytes"], {g["char"]}):
            return {"status": "fail", "note": f"Expected {g['chars']} chars, {g['bytes']} bytes, all {g['char']!r}; "
                    f"got {got[0]} chars, {got[1]} bytes, characters {sorted(got[2])[:5]}."}
        t = fx["trash"]
        rows = query(f"SELECT `path`, `data` FROM {name(t['namespace'], t['table'])} WHERE `id` = '{fx['event']}'")[0]
        binned = [r["data"] for r in rows if (r["path"] or "").endswith(b["path_suffix"])]
        if not binned:
            return {"status": "fail", "note": f"The trash table has no row with id = {fx['event']} and that path ({len(rows)} rows for the id)."}
        if not isinstance(binned[0], bytes) or binned[0].decode("utf-8") != b["data"]:
            return {"status": "fail", "note": f"The binned data: expected {b['data']!r}, got {binned[0][:80]!r}."}
        return {"status": "ok", "data": {"chars": got[0], "bytes": got[1]}}

    def schema_evolution():
        v = ex["evolution"]
        run = run_row(f"`{v['column']}`")[0][v["column"]]
        baseline = run_row(f"`{v['column']}`", v["baseline_run"])[0][v["column"]]
        if run != v["run"] or baseline is not None:
            return {"status": "fail", "note": f"Expected {v['run']!r} on the run's row and NULL on the baseline row, got {run!r} and {baseline!r}."}
        return {"status": "ok"}

    def incremental_read():
        posted = datetime.fromisoformat(fx["posted"]).timestamp() * 1000
        snapshots = query(f"SELECT `snapshot_id` AS id, MILLISECOND_TIMESTAMP(`committed_at`) AS ms, `operation` AS op "
                          f"FROM {name(fx['table']['namespace'], fx['table']['table'] + '$snapshots')} ORDER BY `committed_at`")[0]
        before = [s["id"] for s in snapshots if s["ms"] < posted]
        after = [s["id"] for s in snapshots if s["ms"] >= posted and s["op"] == "append"]
        if not before or not after:
            return {"status": "inconclusive", "note": "The fixture table has no snapshot before fixture.posted or no append after it."}
        start, end = before[-1], after[0]
        rows = query(f"SELECT `runId` FROM {fixture}@incr('startSnapshotId'='{start}', 'endSnapshotId'='{end}')")[0]
        run_ids = [r["runId"] for r in rows]
        d = {"from": str(start), "to": str(end), "rows": len(run_ids)}
        if run_ids == [fx["run_id"]]:
            return {"status": "ok", "data": d}
        total = count(f"SELECT COUNT(*) AS n FROM {fixture}")
        if len(run_ids) == total:
            return {"status": "na", "data": d, "note": f"{INCREMENTAL_DOCS}; on an Iceberg table Doris accepts @incr and "
                    f"ignores it, returning all {total} rows of the table."}
        return {"status": "fail", "data": d, "note": f"Expected the one row with runId {fx['run_id']}, got {len(run_ids)} rows with runIds {run_ids[:5]}."}

    def views():
        v = plan["view"]
        sql = "SELECT COUNT(*) AS n FROM {} WHERE " + f"{device} AND {in_window}"
        try:
            view = count(sql.format(name(v["namespace"], v["view"])))
        except pymysql.MySQLError as e:
            if "Cannot get engine-name for view" not in str(e):
                raise
            return {"status": "fail", "note": f"Doris {version} raises \"Cannot get engine-name for view\" because the view "
                    "versions record no engine-name, which the Iceberg view spec makes optional; apache/doris#69829 "
                    "(merged to master on 2026-11-02) loads such views."}
        d = {"view": view, "table": count(sql.format(measurement))}
        if d["view"] != d["table"]:
            return {"status": "fail", "data": d, "note": f"The view returns {d['view']} rows, its base table {d['table']}."}
        if d["table"] == 0:
            return {"status": "inconclusive", "data": d, "note": "The base table has no rows for the device in the window."}
        return {"status": "ok", "data": d}

    checks = {}
    for check_id, check in [
        ("catalog-login", catalog_login), ("s3", s3), ("cdc-tables", cdc_tables), ("latest-tables", latest_tables),
        ("identifiers", identifiers), ("views", views), ("decimal", decimal), ("schema-evolution", schema_evolution),
        ("nesting", nesting), ("large-values", large_values), ("incremental-read", incremental_read),
        ("timestamps", timestamps),  # last: it sets the session time zone
    ]:
        try:
            checks[check_id] = check()
        except Exception as e:  # a failing check must not stop the others
            note = str(e) if isinstance(e, Fail) else f"{type(e).__name__}: {e}"
            checks[check_id] = {"status": "fail", "note": scrub(" ".join(note.split()))[:400]}
        print(check_id, checks[check_id]["status"], file=sys.stderr)

    out = {"client": "doris", "version": version,
           "build": f"{os.environ.get('DORIS_BUILD', f'Apache Doris {version}')}, {server.split()[-1]}, "
                    f"pymysql {metadata.version('pymysql')}",
           "checks": checks}
    conn.close()
    Path(out_path).write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
