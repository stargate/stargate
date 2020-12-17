# SQL API Extension to Stargate

This API extension uses the [Apache Calcite Avatica](https://calcite.apache.org/avatica/) server t
o allow accessing data in [Stargate](https://stargate.io/) by means of SQL queries.

[Apache Calcite](https://calcite.apache.org/) is used for SQL parsing and query planning.

## Usage

1. Set up a [Stargate](https://github.com/stargate/stargate) runtime environment.
1. Run `./mvnw clean package` (at the repository root level) 
1. Start Stargate via the `starctl` command.
    * By default Stargate will listen for SQL clients following the PostgreSQL 
      [wire protocol](https://www.postgresql.org/docs/13/protocol.html) on port 5432.
    * Note: even though Stargate accepts connections from PostgreSQL clients, it is _not_ a drop-in
      replacement for PostgreSQL.
1. Use the [psql](https://www.postgresql.org/docs/current/app-psql.html) client to connect to 
   Stargate and submit SQL queries from the command line.
    * It is also possible to connect from a custom application through the
      [PostgreSQL JDBC driver](https://jdbc.postgresql.org/).
    * Note: PostgreSQL clients for other programming languages should general work too,
      but they are not currently tested.

Note: Authentication is not supported yet.

## SQL Support

At this point only SELECT, UPDATE and DELETE statements are supported.
Query plans are not optimized yet and in most cases will result in full table scans at
the persistence layer.

DDL is not currently supported. Tables should be created using CQL.

## psql example

```shell
$ psql -h localhost -p 5432 test
WARNING:  PostgreSQL protocol support is experimental in Stargate
psql (9.5.24, server 13.0)
WARNING: psql major version 9.5, server major version 13.
         Some psql features might not work.
Type "help" for help.

test=> select distinct keyspace_name from "system_schema"."columns";
   keyspace_name    
--------------------
 system_traces
 system
 system_distributed
 system_schema
 data_endpoint_auth
 system_auth
(6 rows)
```