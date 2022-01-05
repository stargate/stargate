# Persistence Cassandra 3.11

This module represents the implementation of the [persistence-api](../persistence-api) for the Cassandra `3.11.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `3.11.11`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `cassandra.version` property in the [pom.xml](pom.xml).
* Check what is the version of the `com.datastax.cassandra:cassandra-driver-core` in the `org.apache.cassandra:cassandra-all` for the updated version. 
This dependency is set as optional in the `cassandra-all`, but we need it to correctly handle UDFs.
Set the version of the driver in the `cassandra.bundled-driver.version` property in the [pom.xml](pom.xml).
* Change the version in the [Cassandra311MetricsRegistryTest.java](src/test/java/org/apache/cassandra/metrics/Cassandra311MetricsRegistryTest.java) to the new one.
* Check if the new version has a transitive dependency to `org.apache.cassandra:cassandra-thrift`, and if it does remove that dependency from our [pom.xml](pom.xml).
The `cassandra-thrift` was explicitly added when updating to `3.11.11` as it was not anymore in the `cassandra-all`.
* Update the [CI Dockerfile](../ci/Dockerfile) and set the new version in the `ccm create` command related to 3.11.
* Make sure everything compiles and CI tests are green.
* Update the [DEVGUIDE.md](../DEV_GUIDE.md) and replace the old version in the examples.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `3.11.9 -> 3.11.11` [stargate/stargate#1507](https://github.com/stargate/stargate/pull/1507)
* `3.11.8 -> 3.11.9` [stargate/stargate#1337](https://github.com/stargate/stargate/pull/1337) & [stargate/stargate#1346](https://github.com/stargate/stargate/pull/1346)
* `3.11.6 -> 3.11.8` [stargate/stargate#938](https://github.com/stargate/stargate/pull/938)
