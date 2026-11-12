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

# Settings that a platform's job definition can carry: they hold no secret, and Spark reads them only at start-up.
SESSION_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Spark's default redaction pattern misses "credential", the key that holds the client secret.
    "spark.redaction.regex": "(?i)secret|password|token|credential",
}


def catalog_conf(settings=os.environ):
    """The settings of the catalog c8y, from the settings named in the README.

    They include the client secret. A managed Spark service's job reads it from the platform's secret store and sets
    these in its running session with spark.conf.set, never in a job definition that the platform logs.
    """
    return {
        "spark.sql.catalog.c8y": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.c8y.type": "rest",
        "spark.sql.catalog.c8y.uri": settings["CATALOG_URI"],
        "spark.sql.catalog.c8y.warehouse": settings["CATALOG_WAREHOUSE"],
        "spark.sql.catalog.c8y.rest.auth.type": "oauth2",
        "spark.sql.catalog.c8y.credential": settings["CATALOG_CREDENTIAL"],
        "spark.sql.catalog.c8y.oauth2-server-uri": settings["CATALOG_TOKEN_URI"],
        "spark.sql.catalog.c8y.scope": settings["CATALOG_SCOPE"],
        "spark.sql.catalog.c8y.header.X-Iceberg-Access-Delegation": "vended-credentials",
    }


def spark_session(extra=None):
    builder = SparkSession.builder.appName("c8y-iceberg").config(
        "spark.jars.packages",
        f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}",
    )
    for key, value in {**SESSION_CONF, **catalog_conf(), **(extra or {})}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


if __name__ == "__main__":
    spark = spark_session()
    spark.sql("SELECT * FROM c8y.cdc_measurement.c8y_Temperature LIMIT 5").show(truncate=False)
