# Google Cloud Managed Service for Apache Spark

A Spark batch in your Google Cloud project reads the temperature measurements of your tenant from the Cumulocity
lakehouse, calculates hourly statistics per device, and writes them to an Iceberg table in the BigLake catalog of
your project. BigQuery queries that table.

```
Cumulocity tenant catalog              Managed Service for Apache Spark   BigLake Iceberg REST catalog
iceberg.<env-domain>/api/catalog  -->  gold.py                       -->  <catalog>.<namespace>.temperature_hourly
cdc_measurement.c8y_Temperature        (runtime 2.3, Spark 3.5)           queried in BigQuery
(data on Amazon S3)                                                       (data on Cloud Storage)
```

Google renamed Dataproc and Serverless for Apache Spark to Managed Service for Apache Spark, and BigLake metastore
to Lakehouse runtime catalog. The `gcloud` commands, APIs, and IAM roles keep the old names, `dataproc` and
`biglake`.

## Files

| File | Purpose |
|---|---|
| `example.py` | Connects a batch to the tenant's catalog and reads a few rows |
| `gold.py` | The job: hourly statistics per device into a BigLake Iceberg table |
| `checks_job.py`, `run.sh` | The tests, which run the checks of the Apache Spark client and the check on the copy in a batch |
| `setup.sh` | The setup of the test project: steps 4 and 5 below, plus the login of the tests' GitHub workflow |

## Setup

### 1. Create a read-only catalog principal in Cumulocity

Use a Cumulocity user with the role `ROLE_OFFLOADING_ADMIN` (global role OFFLOADING_ADMINISTRATOR). The
principal's name may contain letters and digits only.

```shell
curl -s -X POST "https://<tenant-domain>/service/offloading/api/v1/principals/<name>" -u "<user>:<password>"
```

The response holds `clientId` and `clientSecret`. Cumulocity shows the secret only once. The job needs both, joined
as `<clientId>:<clientSecret>`. Give each platform a principal of its own, so that you can revoke one without
touching the others.

### 2. Set the variables of the commands below

```shell
export GCP_PROJECT=<project>
export REGION=europe-west1                        # a region, not a multi-region
export BIGLAKE_CATALOG=<catalog>                  # the BigLake catalog, named after its bucket
export NAMESPACE=<namespace>                      # the namespace of the copy
export STAGING_BUCKET=${GCP_PROJECT}-c8y-spark    # job files and the AWS SDK bundle
export SUBNET=default
export C8Y_CATALOG_URI=https://iceberg.<environment-domain>/api/catalog
export C8Y_TENANT=<tenant-id>
export C8Y_SECRET=projects/${GCP_PROJECT}/secrets/c8y-catalog-credential
export SA=c8y-spark@${GCP_PROJECT}.iam.gserviceaccount.com
export BUNDLE=gs://${STAGING_BUCKET}/jars/iceberg-aws-bundle-1.10.1.jar
```

### 3. Store the catalog credential in Secret Manager

```shell
gcloud services enable secretmanager.googleapis.com dataproc.googleapis.com --project=$GCP_PROJECT
read -rs C8Y_CRED   # paste <clientId>:<clientSecret>; this keeps it out of the shell history
printf '%s' "$C8Y_CRED" | gcloud secrets create c8y-catalog-credential --data-file=- --project=$GCP_PROJECT
unset C8Y_CRED
```

### 4. Create the service account that the batches run as

```shell
gcloud iam service-accounts create c8y-spark --display-name="Cumulocity lakehouse copy" --project=$GCP_PROJECT

for role in roles/dataproc.worker roles/biglake.viewer; do
  gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$SA --role=$role
done

gcloud secrets add-iam-policy-binding c8y-catalog-credential --project=$GCP_PROJECT \
  --member=serviceAccount:$SA --role=roles/secretmanager.secretAccessor

# Lets you submit batches that run as this account.
gcloud iam service-accounts add-iam-policy-binding $SA --project=$GCP_PROJECT \
  --member=user:$(gcloud config get account) --role=roles/iam.serviceAccountUser
```

IAM can take a minute before it accepts a new service account in a policy. If a grant fails with "does not
exist", run it again.

### 5. Create the staging bucket, the AWS SDK bundle, and the namespace of the copy

```shell
gcloud storage buckets create gs://$STAGING_BUCKET --location=$REGION --uniform-bucket-level-access \
  --public-access-prevention --project=$GCP_PROJECT
gcloud storage buckets add-iam-policy-binding gs://$STAGING_BUCKET \
  --member=serviceAccount:$SA --role=roles/storage.objectAdmin

curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar |
  gcloud storage cp - $BUNDLE

gcloud biglake iceberg namespaces create $NAMESPACE --catalog=$BIGLAKE_CATALOG --project=$GCP_PROJECT
gcloud biglake iceberg namespaces get-iam-policy $NAMESPACE --catalog=$BIGLAKE_CATALOG --project=$GCP_PROJECT \
  --format=json | jq --arg m serviceAccount:$SA '.bindings += [{"role": "roles/biglake.editor", "members": [$m]}]' \
  > ns-policy.json
gcloud biglake iceberg namespaces set-iam-policy $NAMESPACE ns-policy.json \
  --catalog=$BIGLAKE_CATALOG --project=$GCP_PROJECT
```

Runtime 2.3 brings Iceberg, with its Spark catalog and its Cloud Storage access, but not the AWS SDK that Iceberg
needs to read the lakehouse on Amazon S3. That SDK is the bundle. The service account needs no access to the bucket
of the BigLake catalog, or to the lakehouse's bucket: both catalogs hand out short-lived storage credentials.

### 6. Give the subnet internet egress

Batches have internal IP addresses only. With Private Google Access they reach Google APIs, but the job also calls
the Cumulocity catalog and reads data files from Amazon S3, so the subnet needs Cloud NAT. Skip this step if the
subnet has it already.

```shell
gcloud compute networks subnets update $SUBNET --region=$REGION --enable-private-ip-google-access \
  --project=$GCP_PROJECT
gcloud compute routers create c8y-spark-router --network=default --region=$REGION --project=$GCP_PROJECT
gcloud compute routers nats create c8y-spark-nat --router=c8y-spark-router --region=$REGION \
  --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges --project=$GCP_PROJECT
```

The subnet also needs a firewall rule that allows traffic between its own addresses. The `default` network has one,
`default-allow-internal`.

### 7. Read a few rows

```shell
gcloud dataproc batches submit pyspark example.py --project=$GCP_PROJECT --region=$REGION --version=2.3 \
  --subnet=$SUBNET --service-account=$SA --deps-bucket=gs://$STAGING_BUCKET --jars=$BUNDLE -- \
  --catalog-uri=$C8Y_CATALOG_URI --warehouse=$C8Y_TENANT --secret=$C8Y_SECRET
```

`gcloud` uploads the file to the staging bucket, waits for the batch, and shows the driver's output, which ends
with five rows of `cdc_measurement.c8y_Temperature`.

### 8. Write the copy

```shell
gcloud dataproc batches submit pyspark gold.py --py-files=example.py --project=$GCP_PROJECT --region=$REGION \
  --version=2.3 --subnet=$SUBNET --service-account=$SA --deps-bucket=gs://$STAGING_BUCKET --jars=$BUNDLE -- \
  --catalog-uri=$C8Y_CATALOG_URI --warehouse=$C8Y_TENANT --secret=$C8Y_SECRET \
  --gcp-project=$GCP_PROJECT --biglake-catalog=$BIGLAKE_CATALOG --namespace=$NAMESPACE
```

The last line of the output reads `Wrote <n> rows to lake.<namespace>.temperature_hourly`. By default the job reads
the last 30 days; change this with `--days`. Devices that follow the Cumulocity sensor library send
`c8y_TemperatureMeasurement` instead of `c8y_Temperature`; pass `--fragment` and `--series` if your tenant uses
other names. A rerun replaces the rows of the previous run.

### 9. Query the copy in BigQuery

BigQuery addresses a table of a BigLake Iceberg REST catalog as `project.catalog.namespace.table`:

```sql
SELECT device_id, hour, avg_value, min_value, max_value, samples, unit
FROM `<project>.<catalog>.<namespace>.temperature_hourly`
WHERE device_id = '<device-id>'
ORDER BY hour DESC;
```

People who query the table need `roles/biglake.viewer` on the namespace and `roles/bigquery.jobUser` on the project.

## Output table

| Column | Type | Content |
|---|---|---|
| `device_id` | STRING | Managed object ID of the device (`source`) |
| `hour` | TIMESTAMP | Start of the UTC hour |
| `avg_value`, `min_value`, `max_value` | DOUBLE | Statistics of the series values in that hour |
| `samples` | BIGINT | Number of measurements in that hour |
| `unit` | STRING | Unit of the series |

The lakehouse stores values as `decimal(38,9)`. The job casts the statistics to DOUBLE, which BigQuery reads as
FLOAT64.

## Things to know

* **Versions.** Runtime 2.3 loads its own Iceberg ahead of any Iceberg JAR file passed with `--jars`, so staging
  another Iceberg version has no effect. Google does not document which Iceberg version the runtime brings. On
  2026-11-12, runtime 2.3.43 loaded `iceberg-spark-runtime-3.5_2.13-1.9.0.jar`, built from commit `f73689b6`, which
  the Apache Iceberg repository does not contain; that build reports its version as `unspecified`. The test result
  in the table names the version of the last run.
* **Token lifetimes.** Iceberg renews the Cumulocity catalog token and the storage credentials of the lakehouse
  itself. The job fetches the BigLake token once, and it is valid for about an hour. For a job that runs longer,
  set `rest.auth.type` of the catalog `lake` to `org.apache.iceberg.gcp.auth.GoogleAuthManager`, which renews it
  and needs Iceberg 1.10 or later.
* **A warning about the OAuth2 server URI.** Iceberg 1.9 and older log that the "Iceberg REST client is missing the
  OAuth2 server URI configuration" whenever the token endpoint lies under the catalog URI, as it does for
  Cumulocity, even though the job sets the endpoint.
* **Deletes.** The job counts `MEASUREMENT_CREATE` rows only. Streaming Lake Ingestion does not record measurement
  deletes yet, so measurements deleted in Cumulocity still count toward the statistics.
* **Data transfer.** Spark reads the data files from Amazon S3 into Google Cloud. The time filter limits the read to
  the monthly partitions it needs.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Timeout connecting to `iceberg.<environment-domain>` or to Amazon S3 | No Cloud NAT on the subnet (step 6) |
| `401` from `/v1/oauth/tokens` | A wrong `<clientId>:<clientSecret>` in the secret, or the principal was rotated or deleted |
| `NoSuchTableException: cdc_measurement.c8y_Temperature` | The tenant uses another fragment name; list the tables with `SHOW TABLES IN c8y.cdc_measurement` |
| `403` from `biglake.googleapis.com` | No `roles/biglake.editor` on the namespace, or the account cannot bill requests to the project (`x-goog-user-project`); grant `roles/serviceusage.serviceUsageConsumer` |
| `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` | The runtime no longer brings Iceberg; add `iceberg-spark-runtime-3.5_2.13` to `--jars` |
