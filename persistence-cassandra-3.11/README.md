# Persistence Cassandra 3.11

This module represents the implementation of the [persistence-api](../persistence-api) for the Cassandra `3.11.x` version.

**Warning:** Support for Cassandra 3.11 is considered deprecated and will be removed in the Stargate v3 release: [details](https://github.com/stargate/stargate/discussions/2242).

## Cassandra version update

The current Cassandra version this module depends on is `3.11.13`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `cassandra.version` property in the [pom.xml](pom.xml).
* Update the cassandra version in the Docker compose [environment](../docker-compose/cassandra-3.11/.env) 
* Check the transitive dependencies of the `org.apache.cassandra:cassandra-all` for the new version.
Make sure that the version of the `com.datastax.cassandra:cassandra-driver-core` that `cassandra-all` depends on, is same as in the `cassandra.bundled-driver.version` property in the [pom.xml](pom.xml).
This dependency is set as optional in the `cassandra-all`, but we need it to correctly handle UDFs.
Note that transitive dependencies can be seen on [mvnrepository.com](https://mvnrepository.com/artifact/org.apache.cassandra/cassandra-all) or by running `./mvnw dependency:tree -pl persistence-cassandra-3.11`.
* Change the version in the [Cassandra311MetricsRegistryTest.java](src/test/java/org/apache/cassandra/metrics/Cassandra311MetricsRegistryTest.java) to the new one.
* Check if the new version has a transitive dependency to `org.apache.cassandra:cassandra-thrift`, and if it does remove that dependency from our [pom.xml](pom.xml).
The `cassandra-thrift` was explicitly added when updating to `3.11.12` as it was not anymore in the `cassandra-all`.
* Update the [CI Dockerfile](../ci/Dockerfile) and set the new version in the `ccm create` command related to 3.11.
Note that this will have no effect until the docker image is rebuilt and pushed to the remote repository, thus creating an issue for that would be a good idea.
* Create a separate PR for bumping the Cassandra 3 version in the Quarkus-based API integration tests on the `v2.0.0` branch. Test profiles are defined in the `apis/pom.xml`.
* Make sure everything compiles and CI tests are green.
* Update the [DEVGUIDE.md](../DEV_GUIDE.md) and replace the old version in the examples.
* Update the [default docker-compose env variables](../docker-compose/cassandra-3.11/.env) to reference the new version.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `3.11.11 -> 3.11.12` [stargate/stargate#1646](https://github.com/stargate/stargate/pull/1646)
* `3.11.9 -> 3.11.11` [stargate/stargate#1507](https://github.com/stargate/stargate/pull/1507)
* `3.11.8 -> 3.11.9` [stargate/stargate#1337](https://github.com/stargate/stargate/pull/1337) & [stargate/stargate#1346](https://github.com/stargate/stargate/pull/1346)
* `3.11.6 -> 3.11.8` [stargate/stargate#938](https://github.com/stargate/stargate/pull/938)
