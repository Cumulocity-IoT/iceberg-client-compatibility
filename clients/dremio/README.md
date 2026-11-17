# Dremio Enterprise

Cumulocity provisions a Dremio data source for the tenant, named after the tenant ID, with the OAuth client credential
and credential vending. A table is addressed as `<tenant>.<namespace>.<table>`. [`example.py`](example.py) queries it
through Dremio's REST API.

## Values

* `decimal(38,9)`: the REST API returns exact JSON number literals. Read them as decimals; a JSON parser that reads
  numbers as doubles rounds them.
* Timestamps: the REST API has no session time zone and returns the instant in UTC, without a zone.
