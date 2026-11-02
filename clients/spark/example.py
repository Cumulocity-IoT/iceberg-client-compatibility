# /// script
# requires-python = ">=3.10,<3.14"
# dependencies = ["pyspark==4.1.4"]
# ///
"""Connect Apache Spark to the Iceberg catalog of a Cumulocity tenant and read a few rows.

Spark 4.1 runs on Java 17 or 21. The settings come from the environment, see the README.
"""

import os

from pyspark.sql import SparkSession

ICEBERG = "1.12.0"


def spark_session(extra=None):
    builder = (
        SparkSession.builder.appName("c8y-iceberg")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.c8y", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.c8y.type", "rest")
        .config("spark.sql.catalog.c8y.uri", os.environ["CATALOG_URI"])
        .config("spark.sql.catalog.c8y.warehouse", os.environ["CATALOG_WAREHOUSE"])
        .config("spark.sql.catalog.c8y.rest.auth.type", "oauth2")
        .config("spark.sql.catalog.c8y.credential",os.environ["CATALOG_CREDENTIAL"])
        .config("spark.sql.catalog.c8y.oauth2-server-uri", os.environ["CATALOG_TOKEN_URI"])
        .config("spark.sql.catalog.c8y.scope", os.environ["CATALOG_SCOPE"])
        .config("spark.sql.catalog.c8y.header.X-Iceberg-Access-Delegation", "vended-credentials")
        # Spark's default redaction pattern misses "credential", the key that holds the client secret.
        .config("spark.redaction.regex", "(?i)secret|password|token|credential")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


if __name__ == "__main__":
    spark = spark_session()
    spark.sql("SELECT * FROM c8y.cdc_measurement.c8y_Temperature LIMIT 5").show(truncate=False)
