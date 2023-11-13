# Persistence DSE Next

This module represents the implementation of the [persistence-api](../persistence-api) for the next DSE version following 6.8.

Note: This persistence backend includes vector search and SAI indexing features that have been proposed for Cassandra 5 via the Cassandra Enhancement Proposal (CEP) process.

## Cassandra version update

The current Cassandra version this module depends on is `4.0.11-49a1eecaf3e4` from [datastax/cassandra:vsearch](https://github.com/datastax/cassandra/tree/vsearch) (released 2023-11-13)
In order to update to a newer patch version, please follow the guidelines below:

* Update the `cassandra.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-dse-next` profile section) in [testing/pom.xml](../testing/pom.xml) 
* Check the transitive dependencies of the `org.apache.cassandra:cassandra-all` for the new version.
Make sure that the version of the `com.datastax.cassandra:cassandra-driver-core` that `cassandra-all` depends on, is same as in the `cassandra.bundled-driver.version` property in the [pom.xml](pom.xml).
This dependency is set as optional in the `cassandra-all`, but we need it to correctly handle UDFs.
* Update version defined in the `apis/pom.xml`.
* Make sure everything compiles and CI tests are green.
* Update the [default docker-compose env variables](../docker-compose/dse-next/.env) to reference the new version.
* Update this `README.md` file with the new or updated instructions.
