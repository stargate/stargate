# Persistence DSE 7.0

This module represents the implementation of the [persistence-api](../persistence-api) for
the DSE (DataStax Enterprise cassandra) `7.0.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `7.0.0-ALPHA-3`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `dse.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-dse-7.0` profile section) in [testing/pom.xml](../testing/pom.xml)
* Create a separate PR for bumping the DSE version in the Quarkus-based API integration tests on the `v2.0.0` branch. Test profiles are defined in the `apis/pom.xml`.
* Make sure everything compiles and CI tests are green.
* Update the [default docker-compose env variables](../docker-compose/dse-7.0/.env) to reference the new version.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

(Sample to update in the future)
* `6.8.39` -> `6.8.40` [stargate/stargate#2837](https://github.com/stargate/stargate/pull/2837)
