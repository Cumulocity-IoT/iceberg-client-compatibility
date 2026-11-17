# Apache Doris

## Values

Over the MySQL protocol, Doris returns a `TIMESTAMPTZ(6)` as text in the session time zone. To read the instant
exactly, select `MILLISECOND_TIMESTAMP(<column>)`, its epoch milliseconds.
