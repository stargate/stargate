# SQL shell for Stargate

This module combines [SqlLine](https://github.com/julianhyde/sqlline) and
the [Apache Calcite Avatica](https://calcite.apache.org/avatica/) JDBC driver to make an SQL
shell for Stargate.

## Usage

### Starting the shell

```shell script
$ mvn exec:java
~/p/s/s/sqlline-client (add-sql-poc) $ mvn exec:java
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building stargate-sqlline-client 0.0.1-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:3.0.0:java (default-cli) @ stargate-sqlline-client ---
sqlline version 1.9.0
sqlline>
```

### Connecting to Stargate

Obtain an authentication token.
```shell
$ curl -X POST http://localhost:8081/v1/auth/token/generate -H "accept: application/json" -H "Content-Type: application/json" -d '{"key":"cassandra", "secret":"cassandra"}'
{"authToken":"846854d7-45f8-4525-be9d-7d207d7e09e4"}
```

Use the returned token as the JDBC password to connect in `sqlline`.
```
sqlline> !connect jdbc:avatica:remote:url=http://localhost:8765;serialization=PROTOBUF token 846854d7-45f8-4525-be9d-7d207d7e09e4
Transaction isolation level TRANSACTION_REPEATABLE_READ is not supported. Default (TRANSACTION_NONE) will be used instead.
0: jdbc:avatica:remote:url=http://localhost:8>
```

### Executing SQL

Note: this example is based on Apache Cassandra 3.11.8 as the persistence layer.

```
0: jdbc:avatica:remote:url=http://localhost:8> select distinct keyspace_name from "system_schema"."columns";
+---------------+
| keyspace_name |
+---------------+
| system_schema |
| system_auth   |
+---------------+
2 rows selected (1.345 seconds)
```
