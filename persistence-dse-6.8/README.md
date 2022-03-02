# Persistence DSE 6.8

This module represents the implementation of the [persistence-api](../persistence-api) for
the DSE (DataStax Enterprise cassandra) `6.8.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `6.8.20`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `cassandra.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-dse-6.8` profile section) in [testing/pom.xml](testing/pom.xml)
* Update the [CI Dockerfile](../ci/Dockerfile) and set the new version in the `ccm create` command related to DSE 6.8.
Note that this will have no effect until the docker image is rebuilt and pushed to the remote repository, thus creating an issue for that would be a good idea.
* Make sure everything compiles and CI tests are green.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `6.8.16` -> `6.8.20` [stargate/stargate#1652](https://github.com/stargate/stargate/pull/1652)
