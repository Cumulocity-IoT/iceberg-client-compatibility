# /// script
# requires-python = ">=3.10,<3.13"
# dependencies = ["apache-flink==2.3.0"]
# [tool.uv]
# build-constraint-dependencies = ["setuptools<81"]  # apache-beam builds from source on macOS arm64 and needs pkg_resources
# ///
"""Connect Apache Flink (PyFlink, batch mode) to the Iceberg catalog of a Cumulocity tenant and read a few rows.

This Flink runs on Java 11, 17 or 21. The settings come from the environment, see the README. The Iceberg
runtime for the installed Flink and what the REST catalog and S3FileIO need are downloaded from Maven Central.
"""

import os
import urllib.request
from importlib.metadata import version
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

ICEBERG = "1.12.0"
HADOOP = "3.4.3"  # Flink's Iceberg catalog and Parquet reader need Hadoop classes; the version Iceberg builds against
FLINK_MINOR = ".".join(version("apache-flink").split(".")[:2])
JARS = [
    f"org/apache/iceberg/iceberg-flink-runtime-{FLINK_MINOR}/{ICEBERG}/iceberg-flink-runtime-{FLINK_MINOR}-{ICEBERG}.jar",
    f"org/apache/iceberg/iceberg-aws-bundle/{ICEBERG}/iceberg-aws-bundle-{ICEBERG}.jar",
    f"org/apache/hadoop/hadoop-client-api/{HADOOP}/hadoop-client-api-{HADOOP}.jar",
    f"org/apache/hadoop/hadoop-client-runtime/{HADOOP}/hadoop-client-runtime-{HADOOP}.jar",
]


def jars(cache=Path(__file__).resolve().parent / "target" / "jars"):
    cache.mkdir(parents=True, exist_ok=True)
    paths = []
    for jar in JARS:
        path = cache / jar.rsplit("/", 1)[1]
        if not path.exists():
            part = path.with_suffix(".part")
            urllib.request.urlretrieve(f"https://repo1.maven.org/maven2/{jar}", part)
            part.rename(path)
        paths.append(path)
    return paths


def table_env():
    *iceberg, hadoop_api, hadoop_runtime = jars()
    # Hadoop goes on the JVM's own classpath: Iceberg's catalog factory does not find it among the pipeline jars.
    os.environ["HADOOP_CLASSPATH"] = os.pathsep.join(map(str, (hadoop_api, hadoop_runtime)))
    env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())  # starts the JVM
    # Classpaths rather than pipeline.jars: the jars are local, so no job has to upload them to the cluster.
    env.get_config().set("pipeline.classpaths", ";".join(path.as_uri() for path in iceberg))
    options = {
        "type": "iceberg",
        "catalog-type": "rest",
        "uri": os.environ["CATALOG_URI"],
        "warehouse": os.environ["CATALOG_WAREHOUSE"],
        "rest.auth.type": "oauth2",
        "credential": os.environ["CATALOG_CREDENTIAL"],  # clientId:clientSecret
        "oauth2-server-uri": os.environ["CATALOG_TOKEN_URI"],
        "scope": os.environ["CATALOG_SCOPE"],
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }
    quoted = ", ".join("'{}' = '{}'".format(k, v.replace("'", "''")) for k, v in options.items())
    env.execute_sql(f"CREATE CATALOG lake WITH ({quoted})")  # registered in this session only
    return env


if __name__ == "__main__":
    table_env().execute_sql("SELECT * FROM lake.cdc_measurement.c8y_Temperature LIMIT 5").print()
