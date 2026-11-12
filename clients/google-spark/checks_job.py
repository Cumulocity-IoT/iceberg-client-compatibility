"""The test job: the checks of the Apache Spark client and the check on the copy, in a batch.

run.sh submits it with clients/spark/checks.py, harness/redact.py, example.py and gold.py in --py-files and the plan
in --files. It writes its result to the Cloud Storage path in --out, with "iceberg" for the Iceberg version that
loaded; run.sh adds the runtime's version, which the batch's description holds.
"""

import argparse
import json
import os
import platform
import re

from pyspark import SparkFiles
from google.auth.transport.requests import AuthorizedSession

import gold
from checks import error_note, run_checks
from example import add_arguments, connect, google_credentials, read_secret, spark_session


def bigquery(session, project, sql):
    """The first row of a query, as strings."""
    response = session.post(f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
                            json={"query": sql, "useLegacySql": False, "timeoutMs": 120000})
    if not response.ok:
        raise RuntimeError(f"BigQuery: {response.json()['error']['message']}")
    body = response.json()
    if not body.get("jobComplete"):
        raise TimeoutError("BigQuery did not finish the query within 2 minutes.")
    return [cell["v"] for cell in body["rows"][0]["f"]]


def copy(spark, plan, args, session):
    """Writes the copy with gold.py and reads it back with BigQuery."""
    w, m = plan["window"], plan["measurement"]
    rows = gold.hourly(spark, f"c8y.`{m['namespace']}`.`{m['table']}`", w["from"], w["to"])
    hours, samples = rows.selectExpr("count(*)", "coalesce(sum(samples), 0)").collect()[0]
    if hours == 0:
        return {"status": "inconclusive", "note": "The window has no measurements to copy."}
    gold.write(spark, rows, f"lake.`{args.namespace}`.`{args.table}`")
    got = bigquery(session, args.gcp_project, f"SELECT count(*), sum(samples) FROM "
                   f"`{args.gcp_project}.{args.biglake_catalog}.{args.namespace}.{args.table}`")
    d = {"hours": hours, "samples": samples, "bigquery": {"hours": int(got[0]), "samples": int(got[1] or 0)}}
    if (d["bigquery"]["hours"], d["bigquery"]["samples"]) != (hours, samples):
        return {"status": "fail", "data": d, "note": f"BigQuery reads {got[0]} hours with {got[1]} samples from the "
                f"copy, Spark wrote {hours} hours with {samples} samples."}
    return {"status": "ok", "data": d}


def jar(jvm, name):
    """The file name of the JAR that a class loaded from."""
    cls = jvm.py4j.reflection.ReflectionUtil.classForName(name)
    return os.path.basename(cls.getProtectionDomain().getCodeSource().getLocation().getPath())


def upload(session, uri, text):
    bucket, name = uri.removeprefix("gs://").split("/", 1)
    response = session.post(f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o",
                            params={"uploadType": "media", "name": name}, data=text.encode(),
                            headers={"Content-Type": "application/json"})
    response.raise_for_status()


def main():
    parser = gold.add_arguments(add_arguments(argparse.ArgumentParser()))
    parser.add_argument("--out", required=True, help="gs://<bucket>/<path> of the result")
    args = parser.parse_args()

    credentials = google_credentials()
    session = AuthorizedSession(credentials)
    secret = read_secret(args.secret, credentials)
    spark = spark_session("compat-tests")
    with open(SparkFiles.get("plan.json")) as f:  # SparkFiles needs the running session
        plan = json.load(f)
    connect(spark, args, secret)
    gold.connect_lake(spark, args, credentials)
    checks = run_checks(spark, plan, secret)
    try:
        checks["copy"] = copy(spark, plan, args, session)
    except Exception as e:  # the result of the other checks must still be written
        checks["copy"] = {"status": "fail", "note": error_note(e, secret)}
    print("copy", checks["copy"]["status"])

    jvm = spark.sparkContext._jvm
    iceberg, runtime = jvm.org.apache.iceberg.IcebergBuild, jar(jvm, "org.apache.iceberg.spark.SparkCatalog")
    version = iceberg.version()
    if version == "unspecified":  # the runtime's own build of Iceberg; its JAR file's name carries the version
        named = re.search(r"-(\d[\w.]*)\.jar$", runtime)
        version = named.group(1) if named else version
    out = {
        "client": "google-spark",
        "iceberg": version,
        "build": f"Spark {spark.version}, {runtime} (commit {iceberg.gitCommitShortId()}), "
        f"{jar(jvm, 'software.amazon.awssdk.services.s3.S3Client')}, Python {platform.python_version()}",
        "checks": checks,
    }
    upload(session, args.out, json.dumps(out, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
