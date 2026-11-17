# Amazon Redshift

Amazon Redshift Serverless reads the lakehouse through AWS Glue catalog federation, which [`aws/`](../../aws) sets up
with an external schema for each namespace, so a table is addressed as `<namespace>.<table>`.
[`example.py`](example.py) queries the workgroup `REDSHIFT_WORKGROUP` (default `compat`) through the Redshift Data
API with the caller's IAM identity.

## Values

* `decimal(38,9)`: the Data API returns it as text, with the type `numeric(38,9)`; read it as an exact decimal.
* `timestamptz`: Redshift reads it as a timestamp without zone, in UTC, whatever the session time zone.
* Binary values: the Data API returns `varbyte` as base64.
* Structs and lists are `SUPER` values; `json_serialize` returns them as JSON text.
