# Snowflake

Snowflake reads the lakehouse through a catalog integration with the OAuth client credential and credential vending,
and a catalog-linked database on it (`SNOWFLAKE_DATABASE`), whose schemas are the catalog's namespaces. The catalog is
case-sensitive, so [`example.py`](example.py) double-quotes every schema, table, and column name in its exact case.
