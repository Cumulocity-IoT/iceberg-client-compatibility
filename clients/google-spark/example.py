"""Connect a batch of Google Cloud Managed Service for Apache Spark to the Iceberg catalog of a Cumulocity tenant and
read a few rows.

Submit it as the setup guide (README.md) describes:

    gcloud dataproc batches submit pyspark example.py --region=<region> --version=2.3 --subnet=<subnet> \
      --service-account=<account> --jars=gs://<bucket>/jars/iceberg-aws-bundle-1.10.1.jar -- \
      --catalog-uri=https://iceberg.<environment-domain>/api/catalog --warehouse=<tenant-id> \
      --secret=projects/<project>/secrets/<secret>

The batch reads the catalog credential, <clientId>:<clientSecret>, from Secret Manager. The arguments and properties
of a batch show in its description, so the credential is never one of them.
"""

import argparse
import base64

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from pyspark.sql import SparkSession

# Settings that hold no secret and that Spark reads only at start-up.
SESSION_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Spark's default redaction pattern misses "credential" and "authorization", the keys that hold the secrets.
    "spark.redaction.regex": "(?i)secret|password|token|credential|authorization",
    # Hours are UTC hours, whatever the time zone of the batch's machines.
    "spark.sql.session.timeZone": "UTC",
}


def add_arguments(parser):
    parser.add_argument("--catalog-uri", required=True, help="https://iceberg.<environment-domain>/api/catalog")
    parser.add_argument("--warehouse", required=True, help="the tenant ID")
    parser.add_argument("--secret", required=True, help="projects/<project>/secrets/<secret>, holding <clientId>:<clientSecret>")
    parser.add_argument("--token-uri", help="the OAuth2 token endpoint, by default <catalog-uri>/v1/oauth/tokens")
    parser.add_argument("--scope", default="PRINCIPAL_ROLE:ALL")
    return parser


def google_credentials():
    """The batch's service account, with a fresh access token."""
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    return credentials


def read_secret(name, credentials):
    response = AuthorizedSession(credentials).get(f"https://secretmanager.googleapis.com/v1/{name}/versions/latest:access")
    response.raise_for_status()
    return base64.b64decode(response.json()["payload"]["data"]).decode().strip()


def spark_session(app):
    builder = SparkSession.builder.appName(app)
    for key, value in SESSION_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def connect(spark, args, credential):
    """Adds the catalog c8y to the running session, which the batch's description does not show."""
    c8y = "spark.sql.catalog.c8y"
    for key, value in {
        c8y: "org.apache.iceberg.spark.SparkCatalog",
        f"{c8y}.type": "rest",
        f"{c8y}.uri": args.catalog_uri,
        f"{c8y}.warehouse": args.warehouse,
        f"{c8y}.rest.auth.type": "oauth2",
        f"{c8y}.credential": credential,
        f"{c8y}.oauth2-server-uri": args.token_uri or f"{args.catalog_uri}/v1/oauth/tokens",
        f"{c8y}.scope": args.scope,
        f"{c8y}.header.X-Iceberg-Access-Delegation": "vended-credentials",
    }.items():
        spark.conf.set(key, value)


if __name__ == "__main__":
    args = add_arguments(argparse.ArgumentParser()).parse_args()
    spark = spark_session("c8y-iceberg")
    connect(spark, args, read_secret(args.secret, google_credentials()))
    spark.sql("SELECT * FROM c8y.cdc_measurement.c8y_Temperature LIMIT 5").show(truncate=False)
