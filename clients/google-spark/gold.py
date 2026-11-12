"""Hourly temperature statistics per device, from the Cumulocity lakehouse into an Iceberg table of the BigLake catalog
of your project, where BigQuery queries it.

Submit it with example.py in --py-files, as the setup guide (README.md) describes. A rerun replaces the rows of the
previous run.
"""

import argparse
from datetime import datetime, timedelta, timezone

from example import add_arguments as add_catalog_arguments, connect, google_credentials, read_secret, spark_session


def add_arguments(parser):
    parser.add_argument("--gcp-project", required=True, help="the Google Cloud project that BigLake bills")
    parser.add_argument("--biglake-catalog", required=True, help="the BigLake catalog, named after its bucket")
    parser.add_argument("--namespace", required=True, help="the namespace of the copy, created beforehand")
    parser.add_argument("--table", default="temperature_hourly")
    return parser


def connect_lake(spark, args, credentials):
    """Adds the BigLake Iceberg REST catalog lake to the running session.

    The bearer token is valid for about an hour. Iceberg 1.10 and later renew it with
    rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager instead.
    """
    lake = "spark.sql.catalog.lake"
    for key, value in {
        lake: "org.apache.iceberg.spark.SparkCatalog",
        f"{lake}.type": "rest",
        f"{lake}.uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        f"{lake}.warehouse": f"gs://{args.biglake_catalog}",
        f"{lake}.header.Authorization": f"Bearer {credentials.token}",
        f"{lake}.header.x-goog-user-project": args.gcp_project,
        f"{lake}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"{lake}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
    }.items():
        spark.conf.set(key, value)


def hourly(spark, source, start, end, series="T"):
    """The statistics per device and UTC hour of one series of a measurement table, for start <= time < end."""
    value = f"`{series}`.value"
    # The lakehouse stores values as decimal(38,9); DOUBLE is precise enough for an average and is FLOAT64 in
    # BigQuery. The time filter prunes the source table's monthly partitions.
    return spark.sql(f"""
        SELECT source                       AS device_id,
               date_trunc('HOUR', time)     AS hour,
               CAST(avg({value}) AS DOUBLE) AS avg_value,
               CAST(min({value}) AS DOUBLE) AS min_value,
               CAST(max({value}) AS DOUBLE) AS max_value,
               count({value})               AS samples,
               max(`{series}`.unit)         AS unit
        FROM {source}
        WHERE eventType = 'MEASUREMENT_CREATE'
          AND time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}'
          AND {value} IS NOT NULL
        GROUP BY source, date_trunc('HOUR', time)
    """)


def write(spark, rows, target):
    """Replaces the rows of target; the table keeps its identity, so grants on it stay."""
    rows.createOrReplaceTempView("hourly")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            device_id STRING, hour TIMESTAMP, avg_value DOUBLE, min_value DOUBLE, max_value DOUBLE,
            samples BIGINT, unit STRING
        ) USING iceberg
    """)
    spark.sql(f"INSERT OVERWRITE {target} SELECT * FROM hourly")


def main():
    parser = add_arguments(add_catalog_arguments(argparse.ArgumentParser()))
    parser.add_argument("--fragment", default="c8y_Temperature", help="the measurement fragment, one table each")
    parser.add_argument("--series", default="T", help="the series within the fragment")
    parser.add_argument("--days", type=int, default=30, help="how many days back to read")
    args = parser.parse_args()

    credentials = google_credentials()
    spark = spark_session("c8y-temperature-hourly")
    connect(spark, args, read_secret(args.secret, credentials))
    connect_lake(spark, args, credentials)
    end = datetime.now(timezone.utc).replace(microsecond=0)
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = hourly(spark, f"c8y.cdc_measurement.`{args.fragment}`", iso(end - timedelta(days=args.days)), iso(end), args.series)
    target = f"lake.{args.namespace}.{args.table}"
    write(spark, rows, target)
    print(f"Wrote {spark.table(target).count()} rows to {target}")
    spark.stop()


if __name__ == "__main__":
    main()
