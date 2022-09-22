# Persistence DSE 6.8

This module represents the implementation of the [persistence-api](../persistence-api) for
the DSE (DataStax Enterprise cassandra) `6.8.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `6.8.26`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `dse.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-dse-6.8` profile section) in [testing/pom.xml](../testing/pom.xml)
* Update the [CI Dockerfile](../ci/Dockerfile) and set the new version in the `ccm create` command related to DSE 6.8.
Note that this will have no effect until the docker image is rebuilt and pushed to the remote repository, thus creating an issue for that would be a good idea (see below for one such PR)
* Create a separate PR for bumping the DSE version in the Quarkus-based API integration tests on the `v2.0.0` branch. Test profiles are defined in the `apis/pom.xml`.
* Make sure everything compiles and CI tests are green.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `6.8.16` -> `6.8.20` [stargate/stargate#1652](https://github.com/stargate/stargate/pull/1652)
* `6.8.20` -> `6.8.21` [stargate/stargate#1699](https://github.com/stargate/stargate/pull/1699)
* `6.8.21` -> `6.8.24` [stargate/stargate#1898](https://github.com/stargate/stargate/pull/1898)

And PR(s) for Docker image build for Stargate-builders:

* `6.8.24`: [stargate/stargate#1906](https://github.com/stargate/stargate/pull/1906)
