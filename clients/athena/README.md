# Amazon Athena

Athena reads the lakehouse through AWS Glue catalog federation, which [`aws/`](../../aws) sets up.
[`example.py`](example.py) runs its queries in the federated catalog (`GLUE_CATALOG`, by default the tenant ID) and
the Athena workgroup `ATHENA_WORKGROUP` (default `compat`).

## Values

The API returns every value as text, with its type in the column information of the result:

* `decimal(38,9)`: read it as an exact decimal, such as Python's `Decimal`, not as a float.
* `timestamp(6) with time zone`: Athena has no session time zone; the text ends with the zone, `UTC`.
* `varbinary`: pairs of hex digits separated by spaces.
