# Persistence DSE 6.8

This module represents the implementation of the [persistence-api](../persistence-api) for
the DSE (DataStax Enterprise cassandra) `6.8.x` version.

## Cassandra version update

The current Cassandra version this module depends on is `6.8.48`.
In order to update to a newer patch version, please follow the guidelines below:

* Update the `dse.version` property in the [pom.xml](pom.xml).
* Update the `ccm.version` property (`it-dse-6.8` profile section) in [testing/pom.xml](../testing/pom.xml)
* Create a separate PR for bumping the DSE version in the Quarkus-based API integration tests on the `v2.0.0` branch. Test profiles are defined in the `apis/pom.xml`.
* Make sure everything compiles and CI tests are green.
* Update the [default docker-compose env variables](../docker-compose/dse-6.8/.env) to reference the new version.
* Update this `README.md` file with the new or updated instructions.

It's always good to validate your work against the pull requests that bumped the version in the past:

* `6.8.16` -> `6.8.20` [stargate/stargate#1652](https://github.com/stargate/stargate/pull/1652)
* `6.8.31` -> `6.8.32` [stargate/stargate#2430](https://github.com/stargate/stargate/pull/2430)
* `6.8.36` -> `6.8.38` [stargate/stargate#2772](https://github.com/stargate/stargate/pull/2772)
* `6.8.41` -> `6.8.43` [stargate/stargate#2905](https://github.com/stargate/stargate/pull/2905)
* `6.8.43` -> `6.8.47` [stargate/stargate#2929](https://github.com/stargate/stargate/pull/2929)
