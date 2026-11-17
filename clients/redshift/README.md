# Amazon Redshift

Amazon Redshift Serverless reads the lakehouse through AWS Glue catalog federation, which [`aws/`](../../aws)
describes and its `setup.sh` sets up in an AWS account:

* An AWS Glue connection of type `ICEBERGRESTCATALOG` to the tenant's catalog, with the OAuth client credential and
  `CREDENTIAL_VENDING_MODE = SOURCE_VENDED`, and a federated catalog on it, named after the tenant ID.
* For each namespace, a Glue resource link in the default catalog, because Redshift resolves external schemas only
  there, and an external schema with the namespace's name on that link. A table is addressed as
  `<namespace>.<table>`.
* A Redshift Serverless workgroup (`REDSHIFT_WORKGROUP`, default `compat`), whose namespace has an IAM role that may
  read Glue's metadata and call Lake Formation. [`example.py`](example.py) queries it through the Redshift Data API
  with the caller's IAM identity.

Glue federates only the tables whose names have no capitals, such as `latest_inventory.inventory`, and no views.

## Values

* Strings: Redshift reads an Iceberg `string` as `varchar(16384)` and truncates longer strings without an error.
* `decimal(38,9)`: the Data API returns it as text, with the type `numeric(38,9)`; read it as an exact decimal.
* `timestamptz`: Redshift reads it as a timestamp without zone, in UTC, whatever the session time zone.
* Binary values: the Data API returns `varbyte` as base64.
* Structs and lists are `SUPER` values; `json_serialize` returns them as JSON text.
