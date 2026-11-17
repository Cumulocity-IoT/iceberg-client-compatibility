# Amazon Athena

Athena reads the lakehouse through AWS Glue catalog federation, which [`aws/`](../../aws) describes and its
`setup.sh` sets up in an AWS account:

* An AWS Glue connection of type `ICEBERGRESTCATALOG` to the tenant's catalog, with the OAuth client credential and
  `CREDENTIAL_VENDING_MODE = SOURCE_VENDED`, so that Athena reads the data files with the catalog's vended
  credentials.
* A federated catalog on the connection, named after the tenant ID. [`example.py`](example.py) runs its queries in
  that catalog (`GLUE_CATALOG`).
* An Athena workgroup on engine version 3 (`ATHENA_WORKGROUP`, default `compat`).

Glue federates only the tables whose names have no capitals, such as `latest_inventory.inventory`, and no views.

## Values

The API returns every value as text, with its type in the column information of the result:

* `decimal(38,9)`: read it as an exact decimal, such as Python's `Decimal`, not as a float.
* `timestamp(6) with time zone`: Athena has no session time zone; the text ends with the zone, `UTC`.
* `varbinary`: pairs of hex digits separated by spaces.
