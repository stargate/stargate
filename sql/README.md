# SQL API Extension to Stargate

This API extension uses the [Apache Calcite Avatica](https://calcite.apache.org/avatica/) server t
o allow accessing data in [Stargate](https://stargate.io/) by means of SQL queries.

[Apache Calcite](https://calcite.apache.org/) is used for SQL parsing and query planning.

## Usage

1. Set up a [Stargate](https://github.com/stargate/stargate) runtime environment.
1. Run `./mvnw clean package` (at the repository root level) to build the
   `stargate-avatica-server-<version>.jar` OSGi bundle.
1. Copy `stargate-avatica-server-<version>.jar` to the `stargate-lib` directory.
1. Start Stargate via the `starctl` command.
    * Note: by default the Avatica Server will listen on port 8765.
1. Use the [sqlline-client](./sqlline-client/README.md) sub-module to connect to Stargate
   and submit SQL queries from the command line.
    * It is also possible to connect from a custom application through
      [Avatica drivers](https://calcite.apache.org/avatica/docs/index.html#clients).

Note: Authentication is not supported yet.

## SQL Support

At this point only SELECT, UPDATE and DELETE statements are supported.
Query plans are not optimized yet and in most cases will result in full table scans at
the persistence layer.
