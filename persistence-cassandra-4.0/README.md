# Persistence Cassandra 4.0

This module represents the implementation of the [persistence-api](../persistence-api) for the Cassandra `4.0.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `4.0.4`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `cassandra.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-cassandra-4.0` profile section) in [testing/pom.xml](testing/pom.xml)
* Check the transitive dependencies of the `org.apache.cassandra:cassandra-all` for the new version.
Make sure that the version of the `com.datastax.cassandra:cassandra-driver-core` that `cassandra-all` depends on, is same as in the `cassandra.bundled-driver.version` property in the [pom.xml](pom.xml).
This dependency is set as optional in the `cassandra-all`, but we need it to correctly handle UDFs.
Note that transitive dependencies can be seen on [mvnrepository.com](https://mvnrepository.com/artifact/org.apache.cassandra/cassandra-all) or by running `./mvnw dependency:tree -pl persistence-cassandra-4.0`.
* Update the [CI Dockerfile](../ci/Dockerfile) and set the new version in the `ccm create` command related to 4.0.
Note that this will have no effect until the docker image is rebuilt and pushed to the remote repository, thus creating an issue for that would be a good idea.
* Make sure everything compiles and CI tests are green.
* Update the [default docker-compose env variables](../docker-compose/cassandra-4.0/.env) to reference the new version.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `4.0.1` -> `4.0.3` [stargate/stargate#1647](https://github.com/stargate/stargate/pull/1647)
