# Changelog

## [v1.0.77](https://github.com/stargate/stargate/tree/v1.0.77) (2023-06-20)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.15...v1.0.77)

**Fixed bugs:**

- Flaky integration test: SystemTablesTest.addAndRemovePeers [\#984](https://github.com/stargate/stargate/issues/984)

**Closed issues:**

- Update `snappy-java` dep to latest to resolve CVE\(s\) [\#2621](https://github.com/stargate/stargate/issues/2621)
- Remove "error-prone" plug-in [\#2616](https://github.com/stargate/stargate/issues/2616)
- Update to DSE 6.8.36 [\#2612](https://github.com/stargate/stargate/issues/2612)
- DSE docker image fails with no persistence backend found error [\#2605](https://github.com/stargate/stargate/issues/2605)
- Add support for experimental "VectorType" in `persistence-api` [\#2593](https://github.com/stargate/stargate/issues/2593)
- Support CQL access to `CompositeType` / `DynamicCompositeType` values via Stargate [\#2406](https://github.com/stargate/stargate/issues/2406)
- Document population and full search could be restricted to the field paths [\#1024](https://github.com/stargate/stargate/issues/1024)
- Primary key field in Swagger for v2 REST GET operations does not appear [\#1011](https://github.com/stargate/stargate/issues/1011)
- Upgrade swagger to v3 [\#963](https://github.com/stargate/stargate/issues/963)
- Consolidate JSON processing in Documents API [\#948](https://github.com/stargate/stargate/issues/948)
- Consider using spotless for the code format, imports & headers [\#910](https://github.com/stargate/stargate/issues/910)
- Declare complete dependencies in the testing jar [\#897](https://github.com/stargate/stargate/issues/897)
- Distribute gRPC IDL using maven repo instead of submodules [\#842](https://github.com/stargate/stargate/issues/842)
- Provide support for blob column type with REST & GraphQL APIs [\#829](https://github.com/stargate/stargate/issues/829)
- Document API query improvements [\#828](https://github.com/stargate/stargate/issues/828)
- Batched up POST to create multiple rows with the REST API [\#821](https://github.com/stargate/stargate/issues/821)
- Figure out how to deploy protobuf files for gRPC services [\#818](https://github.com/stargate/stargate/issues/818)
- Enable SAI indexes on Cassandra 4 backends in test [\#815](https://github.com/stargate/stargate/issues/815)
- DocumentResourceV2 should return 404 \(not found\) and not 204 \(no content\) in few cases [\#804](https://github.com/stargate/stargate/issues/804)
- Starting multiple Stargate nodes with C\* 3.11 may experience schema disagreement [\#800](https://github.com/stargate/stargate/issues/800)
- Make plugging custom persistence backends easier [\#768](https://github.com/stargate/stargate/issues/768)
- HTTP code for PUT has to be 201 when document is created [\#741](https://github.com/stargate/stargate/issues/741)

**Merged pull requests:**

- closes \#2623: extend prepared with info with var indexes [\#2631](https://github.com/stargate/stargate/pull/2631) ([ivansenic](https://github.com/ivansenic))
- Improve tests wrt \#2577 to verify Map serialization [\#2580](https://github.com/stargate/stargate/pull/2580) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Backport org.json upgrade from SGv2 \(issue \#2543, PR \#2544\) into v1 b… [\#2567](https://github.com/stargate/stargate/pull/2567) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release \(1.0.77-SNAPSHOT\) [\#2566](https://github.com/stargate/stargate/pull/2566) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add default tags [\#2565](https://github.com/stargate/stargate/pull/2565) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.76](https://github.com/stargate/stargate/tree/v1.0.76) (2023-04-26)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.74.1...v1.0.76)

**Merged pull requests:**

- Fix \#2556: upgrade graphql-java dep \(18.3 -\> 18.5\) [\#2557](https://github.com/stargate/stargate/pull/2557) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2551: upgrade DropWizard \(2.0.35 -\> 2.1.6\) [\#2553](https://github.com/stargate/stargate/pull/2553) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- protect against prepared statement cache returning null [\#2552](https://github.com/stargate/stargate/pull/2552) ([ivansenic](https://github.com/ivansenic))

## [v1.0.75](https://github.com/stargate/stargate/tree/v1.0.75) (2023-03-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.74...v1.0.75)

**Closed issues:**

- Use concurrency to cancel workflows on pull request updates [\#2493](https://github.com/stargate/stargate/issues/2493)
- Update to DSE 6.8.33 [\#2490](https://github.com/stargate/stargate/issues/2490)
- organize coordinator modules under coordinator directory [\#2455](https://github.com/stargate/stargate/issues/2455)

**Merged pull requests:**

- increase timeouts for tests [\#2503](https://github.com/stargate/stargate/pull/2503) ([ivansenic](https://github.com/ivansenic))
- Fix \#2477: update DSE dependency 6.8.32 -\> 6.8.33 [\#2497](https://github.com/stargate/stargate/pull/2497) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#2493: cancel existing ci workflows on pull request updates [\#2494](https://github.com/stargate/stargate/pull/2494) ([ivansenic](https://github.com/ivansenic))
- Move coordinator to separate folder on `v1` [\#2492](https://github.com/stargate/stargate/pull/2492) ([ivansenic](https://github.com/ivansenic))
- delete google cloud build files [\#2483](https://github.com/stargate/stargate/pull/2483) ([ivansenic](https://github.com/ivansenic))
- update secrets for release dispatch [\#2482](https://github.com/stargate/stargate/pull/2482) ([ivansenic](https://github.com/ivansenic))
- speed up coord integration tests by ordering tests [\#2481](https://github.com/stargate/stargate/pull/2481) ([ivansenic](https://github.com/ivansenic))
- enable changelog update to run in actions [\#2479](https://github.com/stargate/stargate/pull/2479) ([ivansenic](https://github.com/ivansenic))
- Fix an out-of-date "cassandra-all" dependency [\#2475](https://github.com/stargate/stargate/pull/2475) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix usage of secrets in matrix [\#2469](https://github.com/stargate/stargate/pull/2469) ([ivansenic](https://github.com/ivansenic))
- automated release support: changelog and event dispatching  [\#2457](https://github.com/stargate/stargate/pull/2457) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next release [\#2448](https://github.com/stargate/stargate/pull/2448) ([github-actions[bot]](https://github.com/apps/github-actions))
- use setup-python action in coordinator-test CI [\#2378](https://github.com/stargate/stargate/pull/2378) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.74](https://github.com/stargate/stargate/tree/v1.0.74) (2023-02-15)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.73...v1.0.74)

**Closed issues:**

- InboundHAProxyHandler is not @Sharable (Stargate v1) [\#2446] (https://github.com/stargate/stargate/issues/2446)

**Merged pull requests:**

- make InboundHAProxyHandler sharable \(v1\) [\#2447](https://github.com/stargate/stargate/pull/2447) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- updates the changelog generation for the v1 branch and releases [\#2441](https://github.com/stargate/stargate/pull/2441) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next `v1` release [\#2434](https://github.com/stargate/stargate/pull/2434) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.73](https://github.com/stargate/stargate/tree/v1.0.73) (2023-02-10)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.72...v1.0.73)

**Fixed bugs:**

- Release workflow contains wrong vars [\#2423](https://github.com/stargate/stargate/issues/2423)
- Quarkus starts gRPC server in APIs `v2.0.6` [\#2389](https://github.com/stargate/stargate/issues/2389)

**Closed issues:**

- Update to DSE 6.8.32 [\#2428](https://github.com/stargate/stargate/issues/2428)
- Update DropWizard dependency to latest compatible \(and matching Jetty version\) [\#2417](https://github.com/stargate/stargate/issues/2417)
- Update GH Actions workflows to replace deprecated actions [\#2405](https://github.com/stargate/stargate/issues/2405)
- Prevent Schema update failure on Stargate due to `CompositeType` / `DynamicCompositeType` inclusion in table definition [\#2144](https://github.com/stargate/stargate/issues/2144)
- stargate in kubernetes Cassandra baremetal [\#595](https://github.com/stargate/stargate/issues/595)
- Tracing is enabled client-side by default? [\#203](https://github.com/stargate/stargate/issues/203)
- CI: Create tests using multiple stargate instances [\#56](https://github.com/stargate/stargate/issues/56)
- Use 3-node cluster as test backend [\#51](https://github.com/stargate/stargate/issues/51)

**Merged pull requests:**

- Update DSE to 6.8.32 [\#2430](https://github.com/stargate/stargate/pull/2430) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix workflow typo to address \#2423 [\#2425](https://github.com/stargate/stargate/pull/2425) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Upgrade DropWizard to 2.0.35 for StargateV1 [\#2419](https://github.com/stargate/stargate/pull/2419) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add helper method for logging in Starter.java \(updated\)  [\#2418](https://github.com/stargate/stargate/pull/2418) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update GH workflows to use supported actions [\#2408](https://github.com/stargate/stargate/pull/2408) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add minimal support for DynamicCompositeType in persistence backends [\#2393](https://github.com/stargate/stargate/pull/2393) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update version to v1.0.73-SNAPSHOT [\#2392](https://github.com/stargate/stargate/pull/2392) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.72](https://github.com/stargate/stargate/tree/v1.0.72) (2023-01-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.71...v1.0.72)

**Fixed bugs:**

- Parameter path warning in REST API [\#2362](https://github.com/stargate/stargate/issues/2362)

**Closed issues:**

- Update DSE to 6.8.31 [\#2356](https://github.com/stargate/stargate/issues/2356)

**Merged pull requests:**

- Bump OSS C\* & DSE versions [\#2370](https://github.com/stargate/stargate/pull/2370) ([msmygit](https://github.com/msmygit))
- Bump DSE version to 6.8.31 and OSS C\* 3.x to 3.11.14 [\#2359](https://github.com/stargate/stargate/pull/2359) ([msmygit](https://github.com/msmygit))
- Update Cassandra 3.11 Persistence with Deprecation notice [\#2336](https://github.com/stargate/stargate/pull/2336) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.71](https://github.com/stargate/stargate/tree/v1.0.71) (2022-12-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.70...v1.0.71)

**Merged pull requests:**

- Bump version to `1.0.71-SNAPSHOT` [\#2298](https://github.com/stargate/stargate/pull/2298) ([github-actions[bot]](https://github.com/apps/github-actions))


## [v1.0.70](https://github.com/stargate/stargate/tree/v1.0.70) (2022-12-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.69...v1.0.70)

**Fixed bugs:**

**Closed issues:**

**Merged pull requests:**

- Revert "Perform authz before query interception" [\#2296](https://github.com/stargate/stargate/pull/2296) ([ivansenic](https://github.com/ivansenic))

## [v1.0.69](https://github.com/stargate/stargate/tree/v1.0.69) (2022-12-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.68...v1.0.69)

**Fixed bugs:**

**Closed issues:**

- Update DSE to 6.8.29 [\#2236](https://github.com/stargate/stargate/issues/2236)
- InboundHAProxyHandler is not @Sharable [\#2232](https://github.com/stargate/stargate/issues/2232)
- Update gRPC version on coordinator to `1.49.2` [\#2224](https://github.com/stargate/stargate/issues/2224)
- Perform authz before query interception [\#2198](https://github.com/stargate/stargate/issues/2198)
- Update DSE to 6.8.28 [\#2197](https://github.com/stargate/stargate/issues/2197)

**Merged pull requests:**

- organize dependencies in the DSE [\#2270](https://github.com/stargate/stargate/pull/2270) ([ivansenic](https://github.com/ivansenic))
- fixing missing netty-codec dependency for dse [\#2259](https://github.com/stargate/stargate/pull/2259) ([ivansenic](https://github.com/ivansenic))
- dse version upgraded to 6.8.29 [\#2251](https://github.com/stargate/stargate/pull/2251) ([versaurabh](https://github.com/versaurabh))
- Update grpc.version from 1.45.1 to 1.49.2 [\#2240](https://github.com/stargate/stargate/pull/2240) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Upgrade dse version to 6.8.28 [\#2204](https://github.com/stargate/stargate/pull/2204) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.68](https://github.com/stargate/stargate/tree/v1.0.68) (2022-11-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.67...v1.0.68)

**Fixed bugs:**

- REST API tests fail with `Abrupt GOAWAY closed sent stream` [\#2209](https://github.com/stargate/stargate/issues/2209)

**Closed issues:**

- Update `graphql-java` from 18.1 to 18.3 as per Dependabot alert [\#2189](https://github.com/stargate/stargate/issues/2189)
- gRPC service exception handlers should log exceptions [\#2166](https://github.com/stargate/stargate/issues/2166)
- Move common and shared configuration to the `quarkus-common` `application.yaml` [\#2159](https://github.com/stargate/stargate/issues/2159)
- Rename `master` branch to `main` [\#1490](https://github.com/stargate/stargate/issues/1490)

**Merged pull requests:**

- Fix release version for v1 [\#2235](https://github.com/stargate/stargate/pull/2235) ([versaurabh](https://github.com/versaurabh))
- Bump graphql-java from 18.1 to 18.3 in /graphqlapi on `v1` [\#2230](https://github.com/stargate/stargate/pull/2230) ([ivansenic](https://github.com/ivansenic))
- Allow legacy flush to be used with property [\#2203](https://github.com/stargate/stargate/pull/2203) ([tjake](https://github.com/tjake))

## [v1.0.67](https://github.com/stargate/stargate/tree/v1.0.67) (2022-10-24)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.67...v1.0.67)

**Merged pull requests:**

- Update DropWizard 2.0.32-\>2.0.34 to get commons-text upgraded [\#2188](https://github.com/stargate/stargate/pull/2188) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- enforce JDK 8 [\#2183](https://github.com/stargate/stargate/pull/2183) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.66](https://github.com/stargate/stargate/tree/v1.0.66) (2022-09-29)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.65...v1.0.66)

**Closed issues:**

- Enable gRPC Server Reflection to help with API Consumer on-boarding [\#2068](https://github.com/stargate/stargate/issues/2068)
- Update DSE to 6.8.26 [\#2065](https://github.com/stargate/stargate/issues/2065)

**Merged pull requests:**

- relates to \#2065: bump DSE version to 6.8.26  [\#2094](https://github.com/stargate/stargate/pull/2094) ([ivansenic](https://github.com/ivansenic))
- closes \#2068: Enable gRPC reflection [\#1853](https://github.com/stargate/stargate/pull/1853) ([mpenick](https://github.com/mpenick))

## [v1.0.65](https://github.com/stargate/stargate/tree/v1.0.65) (2022-09-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.64...v1.0.65)

**Closed issues:**

- Update SnakeYAML dependency to 1.32 to resolve CVE-2022-38725 [\#2078](https://github.com/stargate/stargate/issues/2078)
- Improve `docker-compose` scripts to wait for Coordinator to start up before APIs [\#2076](https://github.com/stargate/stargate/issues/2076)
- Failure to create table with nested `Map` type [\#2062](https://github.com/stargate/stargate/issues/2062)
- Failure to support deeply nested structured datatypes by REST API \(in SGv2\) [\#2061](https://github.com/stargate/stargate/issues/2061)

**Merged pull requests:**

- Upgrade SnakeYAML dependency to 1.32 for CVE-2022-38725 [\#2079](https://github.com/stargate/stargate/pull/2079) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Upgrade Jackson from 2.12.7 to 2.13.4; snakeyaml to 1.0.31 [\#2066](https://github.com/stargate/stargate/pull/2066) ([tatu-at-datastax](https://github.com/tatu-at-datastax))


## [v1.0.64](https://github.com/stargate/stargate/tree/v1.0.64) (2022-08-30)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.63...v1.0.64)

**Closed issues:**

- Allow `dse.yaml` to be passed to DSE persistence backend [\#2011](https://github.com/stargate/stargate/issues/2011)
- Cql client encryption is not working [\#1001](https://github.com/stargate/stargate/issues/1001)

**Merged pull requests:**

- include auth excepion message in the thrown exception [\#2046](https://github.com/stargate/stargate/pull/2046) ([ivansenic](https://github.com/ivansenic))
- Add ability to load `dse.yaml` when using DSE persistence [\#2013](https://github.com/stargate/stargate/pull/2013) ([mpenick](https://github.com/mpenick))
- Fix TLS \(`client\_encryption\_options`\) configuration for CQL [\#1992](https://github.com/stargate/stargate/pull/1992) ([mpenick](https://github.com/mpenick))

## [v1.0.63](https://github.com/stargate/stargate/tree/v1.0.63) (2022-08-15)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.61...v1.0.63)

**Fixed bugs:**

- `NullPointerException` on the `RowResource` [\#1994](https://github.com/stargate/stargate/issues/1994)
- Documents API should handle `OverloadedException` gracefully [\#1972](https://github.com/stargate/stargate/issues/1972)
- GET by primary key with frozen\<udt\> using REST V1 is returning 500 error [\#205](https://github.com/stargate/stargate/issues/205)

**Closed issues:**

- Update DSE to 6.8.25 [\#2022](https://github.com/stargate/stargate/issues/2022)
- CQL module needs to be upgraded [\#2015](https://github.com/stargate/stargate/issues/2015)
- Upgrade DropWizard to latest \(2.0.32\), dependencies [\#1968](https://github.com/stargate/stargate/issues/1968)
- Result structure for REST GET `/v2/schemas/keyspaces` \(and `/v2/schemas/keyspaces/{keyspaceName}`\) different from Swagger model in "simple" topology case [\#1396](https://github.com/stargate/stargate/issues/1396)

**Merged pull requests:**

- fix release workflows input defaults [\#2026](https://github.com/stargate/stargate/pull/2026) ([ivansenic](https://github.com/ivansenic))
- Bumping version to 1.0.63-SNAPSHOT [\#2025](https://github.com/stargate/stargate/pull/2025) ([github-actions[bot]](https://github.com/apps/github-actions))
- Update Stargate-builder image to use DSE 6.8.25 [\#2024](https://github.com/stargate/stargate/pull/2024) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update DSE dependency to 6.8.25 [\#2023](https://github.com/stargate/stargate/pull/2023) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- id-path now accepts a path to a non-string value [\#2020](https://github.com/stargate/stargate/pull/2020) ([EricBorczuk](https://github.com/EricBorczuk))
- Fix for Issue \# 205: Fix for Rest V1 API GET by primary key with frozen udt. [\#2018](https://github.com/stargate/stargate/pull/2018) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#1994: Validation for Rest v1 row update API [\#2009](https://github.com/stargate/stargate/pull/2009) ([maheshrajamani](https://github.com/maheshrajamani))
- release workflows port-back [\#2003](https://github.com/stargate/stargate/pull/2003) ([ivansenic](https://github.com/ivansenic))
- closes \#2001: fixing `StargateV1ConfigurationSourceProviderTest` on the CI [\#2002](https://github.com/stargate/stargate/pull/2002) ([ivansenic](https://github.com/ivansenic))
- improving release v1 github workflow [\#1993](https://github.com/stargate/stargate/pull/1993) ([ivansenic](https://github.com/ivansenic))
- remove codacy result upload in CI [\#1985](https://github.com/stargate/stargate/pull/1985) ([ivansenic](https://github.com/ivansenic))
- closes \#1972: correctly unwrap data store exceptions in the QueryExec… [\#1975](https://github.com/stargate/stargate/pull/1975) ([ivansenic](https://github.com/ivansenic))
- Bumping version to "1.0.62-SNAPSHOT" [\#1973](https://github.com/stargate/stargate/pull/1973) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.61](https://github.com/stargate/stargate/tree/v1.0.61) (2022-07-12)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.60...v1.0.61)

**Closed issues:**

- Upgrade DropWizard to latest \(2.0.32\), dependencies [\#1968](https://github.com/stargate/stargate/issues/1968)
- Allow yaml configuration file overrides per-service [\#1953](https://github.com/stargate/stargate/issues/1953)
- Test out running full IT suite for one backend \(C\*4.0\) using Github Actions [\#1913](https://github.com/stargate/stargate/issues/1913)

**Merged pull requests:**

- Fix for \#1953: add ability to override DropWizard app config file [\#1954](https://github.com/stargate/stargate/pull/1954) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Continuation of CI/IT experiment -- build all 3 backends in parallel [\#1914](https://github.com/stargate/stargate/pull/1914) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1968: update DropWizard dep 2.0.28-\>2.0.32, its dependencies [\#1969](https://github.com/stargate/stargate/pull/1969) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.60](https://github.com/stargate/stargate/tree/v1.0.60) (2022-07-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.59...v1.0.60)

**Fixed bugs:**

- NPE thrown when the system property `stargate.proxy\_protocol.dns\_name` is not provided [\#1934](https://github.com/stargate/stargate/issues/1934)

**Closed issues:**

**Merged pull requests:**

- Revert "Fix `ByteBuf` leak caused by not releasing `HAProxyMessage` \(… [\#1943](https://github.com/stargate/stargate/pull/1943) ([mpenick](https://github.com/mpenick))
- Change generic NPE to IllegalArgumentException with descriptive message [\#1939](https://github.com/stargate/stargate/pull/1939) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.59](https://github.com/stargate/stargate/tree/v1.0.59) (2022-06-30)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.58...v1.0.59)

**Fixed bugs:**

- Rest v2 "Get a Table" endpoint does not correctly report defaultTimeToLive [\#1836](https://github.com/stargate/stargate/issues/1836)
- `ByteBuf` leak in `HAProxyMessageDecoder` \(proxy protocol handling?\) [\#1397](https://github.com/stargate/stargate/issues/1397)

**Closed issues:**

- Update persistence to C\*3.11.13, 4.0.4 [\#1920](https://github.com/stargate/stargate/issues/1920)
- Port read and search paths of the `ReactiveDocumentService` to the `ReadDocumentService` [\#1732](https://github.com/stargate/stargate/issues/1732)
- Javascript: Can't run codegen. [\#1691](https://github.com/stargate/stargate/issues/1691)
- Configure GraphQL-java's `DataFetcherExceptionHandler` to reduce log noise [\#1279](https://github.com/stargate/stargate/issues/1279)
- Expose "default time-to-live" configuration of Tables via `persistence-api` [\#1896](https://github.com/stargate/stargate/issues/1896)
- Bridge does not expose "defaultTTL" for `CqlTable` [\#1881](https://github.com/stargate/stargate/issues/1881)

**Merged pull requests:**

- Build docker build image for c3.11.13 / c4.0.4 [\#1932](https://github.com/stargate/stargate/pull/1932) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Upgrade C\*3 \(3.11.13\) and C\*4 \(4.0.4\) dependencies [\#1926](https://github.com/stargate/stargate/pull/1926) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1836 \(expose default TTL for Table resource\) [\#1917](https://github.com/stargate/stargate/pull/1917) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix link on DSE-persistence README [\#1915](https://github.com/stargate/stargate/pull/1915) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix `ByteBuf` leak caused by not releasing `HAProxyMessage` [\#1910](https://github.com/stargate/stargate/pull/1910) ([mpenick](https://github.com/mpenick))
- Test GH actions for CI [\#1904](https://github.com/stargate/stargate/pull/1904) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Reduce GraphQLJava induced log noise [\#1903](https://github.com/stargate/stargate/pull/1903) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add CLA [\#1902](https://github.com/stargate/stargate/pull/1902) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Update `dse-core` dependency from 6.8.21 to 6.8.24 [\#1898](https://github.com/stargate/stargate/pull/1898) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add `ttl\(\)` method to persistence backends [\#1897](https://github.com/stargate/stargate/pull/1897) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Publish new Stargate-builder \(1.0.8\) for DSE core 6.8.24 [\#1906](https://github.com/stargate/stargate/pull/1906) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.58](https://github.com/stargate/stargate/tree/v1.0.58) (2022-06-15)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.57...v1.0.58)

**Closed issues:**

- Update java-driver-code dependency from 4.13.0 to 4.14.1 [\#1890](https://github.com/stargate/stargate/pull/1890) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.57](https://github.com/stargate/stargate/tree/v1.0.57) (2022-05-26)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.56...v1.0.57)

**Closed issues:**

- Update Stargate "org.json" dependency version [\#1848](https://github.com/stargate/stargate/issues/1848)
- Table creation via REST API does not enforce requested order of clustering columns [\#1841](https://github.com/stargate/stargate/issues/1841)
- Implement `WriteBridgeService` in the Document API V2 [\#1728](https://github.com/stargate/stargate/issues/1728)

**Merged pull requests:**

- Upgrade org.json version to latest (to avoid CVE in deps) [\#1849](https://github.com/stargate/stargate/pull/1849) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Enforce requested clustering column order on REST API [\#1842](https://github.com/stargate/stargate/pull/1842) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.56](https://github.com/stargate/stargate/tree/v1.0.56) (2022-05-20)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.55...v1.0.56)

**Merged pull requests:**

- Make test result uploads not break CI when fail [\#1832](https://github.com/stargate/stargate/pull/1832) ([EricBorczuk](https://github.com/EricBorczuk))
- Remove invalid comma for HOST\_ID in `starctl` [\#1826](https://github.com/stargate/stargate/pull/1826) ([mpenick](https://github.com/mpenick))
- Update `.github/CODEOWNERS` [\#1815](https://github.com/stargate/stargate/pull/1815) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.55](https://github.com/stargate/stargate/tree/v1.0.55) (2022-04-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.54...v1.0.55)

**Closed issues:**

- Revert Persistence.unregisterEventListener [\#1777](https://github.com/stargate/stargate/issues/1777)
- Values.byteBuffer returns an empty buffer [\#1676](https://github.com/stargate/stargate/issues/1676)

**Merged pull requests:**

- Add exception message in ErrorCodeRuntimeException for inclusion in Response [\#1800](https://github.com/stargate/stargate/pull/1800) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Improve Stargate V1 logging by adding more info to help Splunk indexing [\#1798](https://github.com/stargate/stargate/pull/1798) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update maven-compiler-plugin 3.8.1-\>3.10.1 \(for post-Java-8 builds\) [\#1791](https://github.com/stargate/stargate/pull/1791) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update Duzzt dependency, indirect StringTemplate, jacoco \(for Java 9+ support\) [\#1788](https://github.com/stargate/stargate/pull/1788) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix Values.byteBuffer to return non-empty buffer \(fixes \#1676\) [\#1781](https://github.com/stargate/stargate/pull/1781) ([olim7t](https://github.com/olim7t))

## [v1.0.54](https://github.com/stargate/stargate/tree/v1.0.54) (2022-04-18)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.53...v1.0.54)

**Closed issues:**


**Merged pull requests:**

- Update Maven wrapper plugin settings to simplify PR \#1769 [\#1784](https://github.com/stargate/stargate/pull/1784) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update the CI image to include needed v2 changes [\#1774](https://github.com/stargate/stargate/pull/1774) ([ivansenic](https://github.com/ivansenic))
- Upgrade io.grpc dependencies from 1.42.1 to 1.45.1, use grpc-bom [\#1758](https://github.com/stargate/stargate/pull/1758) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Upgrade json-path 2.5.0-\>2.7.0 to get json-smart upgrade [\#1756](https://github.com/stargate/stargate/pull/1756) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- upload dependency update results [\#1754](https://github.com/stargate/stargate/pull/1754) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.53](https://github.com/stargate/stargate/tree/v1.0.53) (2022-03-30)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.52...v1.0.53)

**Implemented enhancements:**

- Enhancement request to include TTL for Documents in Astra DB [\#1414](https://github.com/stargate/stargate/issues/1414)

**Closed issues:**

**Merged pull requests:**

- Netty version update to 4.1.75.Final [\#1751](https://github.com/stargate/stargate/pull/1751) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Updated `jersey-common` dependency to 2.34 for vulns [\#1749](https://github.com/stargate/stargate/pull/1749) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- updating to dropwizard 2.0.28 [\#1748](https://github.com/stargate/stargate/pull/1748) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Upgrade Java driver 4.10-\>4.13 [\#1746](https://github.com/stargate/stargate/pull/1746) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Force C\*4 persistence to use latest Netty \(4.1.75-final\) [\#1745](https://github.com/stargate/stargate/pull/1745) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- updating apollo graphql federation library [\#1744](https://github.com/stargate/stargate/pull/1744) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bump logback-core from 1.2.8 to 1.2.9 [\#1743](https://github.com/stargate/stargate/pull/1743) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update jackson-databind to 2.12.6.1 \(via jackson-bom\) for CVE-2020-36518 [\#1741](https://github.com/stargate/stargate/pull/1741) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove `auto-factory` dependency if possible; or if not, update version from pre-1.0 to 1.0.1 [\#1740](https://github.com/stargate/stargate/pull/1740) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Upgrade java driver 4.9 -\> 4.10, to make gremlin dep optional [\#1739](https://github.com/stargate/stargate/pull/1739) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix prepared query not found in Cassandra 4.0 by adding lock to StargateQueryHandler [\#1714](https://github.com/stargate/stargate/pull/1714) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Change nimbus-jose-jwt version to be "managed" to change version globally [\#1708](https://github.com/stargate/stargate/pull/1708) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add in TTL work, with tests [\#1670](https://github.com/stargate/stargate/pull/1670) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.52](https://github.com/stargate/stargate/tree/v1.0.52) (2022-03-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.51...v1.0.52)

**Closed issues:**

- Update DSE to 6.8.21  [\#1692](https://github.com/stargate/stargate/issues/1692)

**Merged pull requests:**

- updating netty versions [\#1709](https://github.com/stargate/stargate/pull/1709) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update slf4j library version [\#1705](https://github.com/stargate/stargate/pull/1705) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- updating version of nimbus-jose-jwt library [\#1704](https://github.com/stargate/stargate/pull/1704) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix changelog reference for release v1.0.51 to get proper delta [\#1702](https://github.com/stargate/stargate/pull/1702) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1692: updates DSE version to 6.8.21 [\#1699](https://github.com/stargate/stargate/pull/1699) ([ivansenic](https://github.com/ivansenic))
- Add testing of Duration/CqlDuration values wrt REST API [\#1695](https://github.com/stargate/stargate/pull/1695) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.51](https://github.com/stargate/stargate/tree/v1.0.51) (2022-03-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.50...v1.0.51)

**Fixed bugs:**

- REST API serializes `timestamp` values as POJOs: should be ISO-8601 [\#1685](https://github.com/stargate/stargate/issues/1685)
- `ExecuteBatchStreamingTest` fails sporadically for the C\* 4.0 [\#1679](https://github.com/stargate/stargate/issues/1679)

**Closed issues:**

- Update Jackson dependency to 2.12.6 \(from 2.10 and others\) [\#1680](https://github.com/stargate/stargate/issues/1680)

**Merged pull requests:**

- Fix eviction race for prepared statements on Cassandra 4.0 [\#1688](https://github.com/stargate/stargate/pull/1688) ([mpenick](https://github.com/mpenick))
- Fix \#1685 handling of `timestamp` values on REST/json output [\#1686](https://github.com/stargate/stargate/pull/1686) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1680: update Jackson dependency to 2.12.6 [\#1681](https://github.com/stargate/stargate/pull/1681) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.50](https://github.com/stargate/stargate/tree/v1.0.50) (2022-03-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.49...v1.0.50)

**Implemented enhancements:**

- Support more consistency levels in mutation options [\#729](https://github.com/stargate/stargate/issues/729)

**Fixed bugs:**

- Add workaround for `QueryProcessor` race condition \[CASSANDRA-17401\] [\#1655](https://github.com/stargate/stargate/issues/1655)
- `@IfBundleAvailable` does not support nested tests classes [\#1649](https://github.com/stargate/stargate/issues/1649)
- Documents not checked for valid JSON schema on batch write [\#1610](https://github.com/stargate/stargate/issues/1610)
- DocsApi allows for creating json with value and empty key [\#1220](https://github.com/stargate/stargate/issues/1220)

**Closed issues:**

- What fields should my JWT define? [\#1674](https://github.com/stargate/stargate/issues/1674)
- gRPC module missing basic authorization tests [\#1665](https://github.com/stargate/stargate/issues/1665)
- Metrics for non-API endpoints should be reported as different module [\#1660](https://github.com/stargate/stargate/issues/1660)
- gRPC to correctly hande retries in case of the `PreparedQueryNotFoundException` [\#1659](https://github.com/stargate/stargate/issues/1659)
- Update C\*3, C\*4 and DSE dependencies by Stargate [\#1644](https://github.com/stargate/stargate/issues/1644)
- Refactor Rest API Integration tests that test Materialized Views into separate class [\#1639](https://github.com/stargate/stargate/issues/1639)
- Cannot launch tests in Intellij [\#1621](https://github.com/stargate/stargate/issues/1621)
- No operation available for full or partial text search  [\#1195](https://github.com/stargate/stargate/issues/1195)

**Merged pull requests:**

- Populate client info on all queries [\#1671](https://github.com/stargate/stargate/pull/1671) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add basic gRPC/authorization tests [\#1666](https://github.com/stargate/stargate/pull/1666) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1220: json with empty field names not supported [\#1663](https://github.com/stargate/stargate/pull/1663) ([ivansenic](https://github.com/ivansenic))
- closes \#1660: non-api endpoint metrics to be reported with different … [\#1662](https://github.com/stargate/stargate/pull/1662) ([ivansenic](https://github.com/ivansenic))
- closes \#1659: proper handling of retries for unprepared errors in grpc [\#1661](https://github.com/stargate/stargate/pull/1661) ([ivansenic](https://github.com/ivansenic))
- Add a test to verify keyspace creation authnz checks; clean up test a bit [\#1658](https://github.com/stargate/stargate/pull/1658) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Specialize v2 QueryBuilder to generate gRPC Query [\#1657](https://github.com/stargate/stargate/pull/1657) ([olim7t](https://github.com/olim7t))
- updating stargate-jars.zip to match expected structure [\#1653](https://github.com/stargate/stargate/pull/1653) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Third part of \#1644 fix: upgrade `dse-core` 6.8.16 -\> 6.8.20 [\#1652](https://github.com/stargate/stargate/pull/1652) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1649: fixed BundleAvailableCondition to support nested classes [\#1650](https://github.com/stargate/stargate/pull/1650) ([ivansenic](https://github.com/ivansenic))
- Second part of \#1644 fix: C\*4 from 4.0.1 to 4.0.3 upgrade [\#1647](https://github.com/stargate/stargate/pull/1647) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- First part of \#1644: C\*3 from 3.11.11-\>3.11.12 [\#1646](https://github.com/stargate/stargate/pull/1646) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Trivial fix to avoid duplicate "Order" values; make sure name/desc separate [\#1643](https://github.com/stargate/stargate/pull/1643) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- ability to add extra tags for each gRPC call [\#1642](https://github.com/stargate/stargate/pull/1642) ([ivansenic](https://github.com/ivansenic))
- Fix \#1639: refactor Materialized View REST API ITs into separate test class [\#1640](https://github.com/stargate/stargate/pull/1640) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- workflow to complete v2 release tasks  [\#1637](https://github.com/stargate/stargate/pull/1637) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Create docker-login.yml [\#1636](https://github.com/stargate/stargate/pull/1636) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add host ID property, flag, and env var [\#1635](https://github.com/stargate/stargate/pull/1635) ([mpenick](https://github.com/mpenick))
- refactor batch write of documents [\#1617](https://github.com/stargate/stargate/pull/1617) ([ivansenic](https://github.com/ivansenic))
- Add an example for streaming gRPC bi-streaming queries [\#1603](https://github.com/stargate/stargate/pull/1603) ([tomekl007](https://github.com/tomekl007))

## [v1.0.49](https://github.com/stargate/stargate/tree/v1.0.49) (2022-02-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.48...v1.0.49)

**Fixed bugs:**

- Documents checked for valid JSON schema also on update and patch [\#1604](https://github.com/stargate/stargate/issues/1604)

**Closed issues:**

- Split REST/Schema tests out of \(too\) big `RestApiv2Test` class [\#1600](https://github.com/stargate/stargate/issues/1600)
- More gRPC `ValueCodec` implementations should verify validity of data decoded \(ByteBuffer-\>proto Value\) [\#1592](https://github.com/stargate/stargate/issues/1592)

**Merged pull requests:**

- DEV\_GUIDE: Fix outdated test class names [\#1620](https://github.com/stargate/stargate/pull/1620) ([li-boxuan](https://github.com/li-boxuan))
- optimizing reuse of resources in IT [\#1618](https://github.com/stargate/stargate/pull/1618) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- gRPC: Fix conversion of UDT field types [\#1614](https://github.com/stargate/stargate/pull/1614) ([olim7t](https://github.com/olim7t))
- relates to \#1604: fix update of sub-document with JSON schema existing [\#1609](https://github.com/stargate/stargate/pull/1609) ([ivansenic](https://github.com/ivansenic))
- refactor patching of the documents [\#1605](https://github.com/stargate/stargate/pull/1605) ([ivansenic](https://github.com/ivansenic))
- Fix root cause of \#1577 \(along with simple tests\) [\#1597](https://github.com/stargate/stargate/pull/1597) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add new request failure codes [\#1596](https://github.com/stargate/stargate/pull/1596) ([ivansenic](https://github.com/ivansenic))
- Add validation for \#1592; also add a test for UUID case \(failing\) [\#1593](https://github.com/stargate/stargate/pull/1593) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update testcontainer, check the @Testcontainers\(disabledWithoutDocker… [\#1588](https://github.com/stargate/stargate/pull/1588) ([ivansenic](https://github.com/ivansenic))
- Improve code to help troubleshoot \#1577, by more validation of UUID codec [\#1586](https://github.com/stargate/stargate/pull/1586) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- use doFinally in the `DseConnection\#executeRequest` to reset warnings [\#1585](https://github.com/stargate/stargate/pull/1585) ([ivansenic](https://github.com/ivansenic))
- updated delete document paths to use new write service [\#1572](https://github.com/stargate/stargate/pull/1572) ([ivansenic](https://github.com/ivansenic))

## [v1.0.48](https://github.com/stargate/stargate/tree/v1.0.48) (2022-01-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.47...v1.0.48)

**Merged pull requests:**

- refactor update document sub-path to use new write service [\#1562](https://github.com/stargate/stargate/pull/1562) ([ivansenic](https://github.com/ivansenic))

## [v1.0.47](https://github.com/stargate/stargate/tree/v1.0.47) (2022-01-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.46...v1.0.47)

**Implemented enhancements:**

- Improve validation fail messages inspired by \#1538 [\#1539](https://github.com/stargate/stargate/pull/1539) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

**Closed issues:**

- Should allow JSON `null` values for inserts via REST API [\#1558](https://github.com/stargate/stargate/issues/1558)
- Failure if trying to create User-Defined Type \(UDT\) with mixed-case name, using REST API [\#1538](https://github.com/stargate/stargate/issues/1538)
- Missing integration tests for REST API operators $EXISTS, $CONTAINS\(|KEY|ENTRY\) [\#1517](https://github.com/stargate/stargate/issues/1517)
- How to run a query \(REST or GraphQL\) with "SELECT count\(\*\)" [\#1514](https://github.com/stargate/stargate/issues/1514)
- SGv2/REST: implement "getWithWhere\(\)" [\#1477](https://github.com/stargate/stargate/issues/1477)
- Feature Request: Configurable allow-list of consistency levels [\#877](https://github.com/stargate/stargate/issues/877)

**Merged pull requests:**

- Update pull\_request\_template.md [\#1563](https://github.com/stargate/stargate/pull/1563) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Upgrade Micrometer and prometheus-client versions in v2.0.0 [\#1560](https://github.com/stargate/stargate/pull/1560) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1558: allow insertion of JSON `null` values via REST API [\#1559](https://github.com/stargate/stargate/pull/1559) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Improve gprc value decode exception messages to help troubleshooting [\#1552](https://github.com/stargate/stargate/pull/1552) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1538: do not verify quoting for Column as it prevents use of mixed-case UDT names [\#1548](https://github.com/stargate/stargate/pull/1548) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove myself from codeowners [\#1546](https://github.com/stargate/stargate/pull/1546) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add Integration Test that inserts+gets a Tuple value \(was missing\) [\#1540](https://github.com/stargate/stargate/pull/1540) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add Integration test to cover UDT insert/get for REST API [\#1536](https://github.com/stargate/stargate/pull/1536) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add test for GET-where/$containsKey and $containsEntry [\#1533](https://github.com/stargate/stargate/pull/1533) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- More REST API test coverage: test with 2 filters for one field \(gt + lt\) [\#1530](https://github.com/stargate/stargate/pull/1530) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1529](https://github.com/stargate/stargate/pull/1529) ([github-actions[bot]](https://github.com/apps/github-actions))
- Support a gRPC bi-directional streaming API [\#1469](https://github.com/stargate/stargate/pull/1469) ([tomekl007](https://github.com/tomekl007))

## [v1.0.46](https://github.com/stargate/stargate/tree/v1.0.46) (2022-01-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.45...v1.0.46)

**Fixed bugs:**

- Persistence API exposes Utf8Type as `varchar` instead of `text` [\#1499](https://github.com/stargate/stargate/issues/1499)

**Closed issues:**

- CI docker file uses old versions when creating and removing ccm [\#1520](https://github.com/stargate/stargate/issues/1520)
- SGv2/REST: refactor `TablesResource`, `KeyspacesResource` to have api/impl separation [\#1483](https://github.com/stargate/stargate/issues/1483)
- SGv2/REST: implement support for "Stringified" versions of structured types \(List, Map, Tuple\) [\#1479](https://github.com/stargate/stargate/issues/1479)
- SGv2/REST: implement `UserDefinedTypesResource` [\#1478](https://github.com/stargate/stargate/issues/1478)
- Support Materialized Views in REST API [\#1324](https://github.com/stargate/stargate/issues/1324)
- Document insert statements should be prepared [\#1282](https://github.com/stargate/stargate/issues/1282)
- Remove cassandra-3.11 dependency from persistence-common [\#781](https://github.com/stargate/stargate/issues/781)
- Ability to set JSON schema on a collection, to be enforced on write [\#613](https://github.com/stargate/stargate/issues/613)

**Merged pull requests:**

- More testing wrt \#1517: $contains for set\<text\> [\#1524](https://github.com/stargate/stargate/pull/1524) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1520: updated CI docker image, correct ccm versions, updated … [\#1521](https://github.com/stargate/stargate/pull/1521) ([ivansenic](https://github.com/ivansenic))
- Add a test for REST API, for existing "where ... $exists" behavior. [\#1518](https://github.com/stargate/stargate/pull/1518) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#781: remove cassandra-all dependency from persistence-commons [\#1510](https://github.com/stargate/stargate/pull/1510) ([ivansenic](https://github.com/ivansenic))
- aligned the DSE version to 6.8.16 in the integration test profile [\#1508](https://github.com/stargate/stargate/pull/1508) ([ivansenic](https://github.com/ivansenic))
- updated persistence-cassandra-3.11 to 3.11.11 [\#1507](https://github.com/stargate/stargate/pull/1507) ([ivansenic](https://github.com/ivansenic))
- Rename Column.Type.Varchar to Text, remove protocol code 10 \(fixes \#1499\) [\#1506](https://github.com/stargate/stargate/pull/1506) ([olim7t](https://github.com/olim7t))
- update and unify dependencies [\#1505](https://github.com/stargate/stargate/pull/1505) ([ivansenic](https://github.com/ivansenic))
- automated code cleanup [\#1504](https://github.com/stargate/stargate/pull/1504) ([ivansenic](https://github.com/ivansenic))
- organized imports in all projects [\#1503](https://github.com/stargate/stargate/pull/1503) ([ivansenic](https://github.com/ivansenic))
- removing reference to google email list [\#1502](https://github.com/stargate/stargate/pull/1502) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Update DropWizard 2.0.21 -\> 2.0.26, deps [\#1497](https://github.com/stargate/stargate/pull/1497) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- write documents with the new shredding and writer [\#1488](https://github.com/stargate/stargate/pull/1488) ([ivansenic](https://github.com/ivansenic))
- remove reflection usage from the request hot path in C4 [\#1476](https://github.com/stargate/stargate/pull/1476) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next release [\#1459](https://github.com/stargate/stargate/pull/1459) ([github-actions[bot]](https://github.com/apps/github-actions))
- fix flaky in testToStringWithCollections [\#1383](https://github.com/stargate/stargate/pull/1383) ([arianacai1997](https://github.com/arianacai1997))
- Support reading rows from a Materialized View [\#1349](https://github.com/stargate/stargate/pull/1349) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.45](https://github.com/stargate/stargate/tree/v1.0.45) (2021-12-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.44...v1.0.45)

**Merged pull requests:**

- gRPC: Set "host" header to ":authority" pseudo-header value [\#1457](https://github.com/stargate/stargate/pull/1457) ([mpenick](https://github.com/mpenick))
- avoid going to props every time for supportsSecondaryIndex [\#1456](https://github.com/stargate/stargate/pull/1456) ([ivansenic](https://github.com/ivansenic))
- Upgrade swagger-ui dependency from 3.35.0 to the latest 3.x, 3.52.5. [\#1452](https://github.com/stargate/stargate/pull/1452) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1449](https://github.com/stargate/stargate/pull/1449) ([github-actions[bot]](https://github.com/apps/github-actions))
- refactored shredding of json payload [\#1439](https://github.com/stargate/stargate/pull/1439) ([ivansenic](https://github.com/ivansenic))

## [v1.0.44](https://github.com/stargate/stargate/tree/v1.0.44) (2021-12-02)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.43...v1.0.44)

**Closed issues:**

- REST `TablesResource.getOneTable\(\)`endpoint swagger claims result is `Table` but is actually `TableResponse` [\#1444](https://github.com/stargate/stargate/issues/1444)
- Implement Stargate V2 REST endpoints for "createTable" using new gRPC endpoint [\#1435](https://github.com/stargate/stargate/issues/1435)
- Implement Stargate V2 REST endpoints for getting table metadata using new gRPC endpoints  [\#1426](https://github.com/stargate/stargate/issues/1426)
- Implement Stargate V2 REST endpoints for keyspace CRUD operations using new gRPC endpoints [\#1425](https://github.com/stargate/stargate/issues/1425)
- Implement "getRows\(\)" \(PK access\) for SGv2 prototype [\#1422](https://github.com/stargate/stargate/issues/1422)
- TracingQueryTest flaky on DSE builds [\#1388](https://github.com/stargate/stargate/issues/1388)

**Merged pull requests:**

- increment gRPC version [\#1447](https://github.com/stargate/stargate/pull/1447) ([tomekl007](https://github.com/tomekl007))
- Fix \#1444 by changing swagger annotated response type to correct one [\#1445](https://github.com/stargate/stargate/pull/1445) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- maybe create table should not check for valid name if it exists [\#1438](https://github.com/stargate/stargate/pull/1438) ([ivansenic](https://github.com/ivansenic))
- increase initial page size for `$or` search in docs api [\#1434](https://github.com/stargate/stargate/pull/1434) ([ivansenic](https://github.com/ivansenic))
- check connection getPrepare before executing the prepare [\#1432](https://github.com/stargate/stargate/pull/1432) ([ivansenic](https://github.com/ivansenic))
- Tiny improvements to REST/getTable IT checks to avoid passing with no columns [\#1430](https://github.com/stargate/stargate/pull/1430) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- doing document population in parallel batches [\#1424](https://github.com/stargate/stargate/pull/1424) ([ivansenic](https://github.com/ivansenic))
- Improve test "getRows\(\)" for RESTv2 test [\#1421](https://github.com/stargate/stargate/pull/1421) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1420](https://github.com/stargate/stargate/pull/1420) ([github-actions[bot]](https://github.com/apps/github-actions))
- closes \#1388: improved channel handling in the gRPC integration tests [\#1419](https://github.com/stargate/stargate/pull/1419) ([ivansenic](https://github.com/ivansenic))

## [v1.0.43](https://github.com/stargate/stargate/tree/v1.0.43) (2021-11-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.42...v1.0.43)

**Closed issues:**

- Add `byte\[\]` taking variant\(s\) in `ByteBufferUtils` for Base64 handling [\#1404](https://github.com/stargate/stargate/issues/1404)
- Implement "addRow\(\)" for SGv2 prototype [\#1399](https://github.com/stargate/stargate/issues/1399)
- Improve JWT documentation [\#1384](https://github.com/stargate/stargate/issues/1384)

**Merged pull requests:**

- Also export logback core from for persistence backends [\#1417](https://github.com/stargate/stargate/pull/1417) ([mpenick](https://github.com/mpenick))
- Implement gRPC createTable operation [\#1411](https://github.com/stargate/stargate/pull/1411) ([olim7t](https://github.com/olim7t))
- Centralize logging [\#1410](https://github.com/stargate/stargate/pull/1410) ([mpenick](https://github.com/mpenick))
- Fix parameterized keyspace regression in the gRPC API [\#1408](https://github.com/stargate/stargate/pull/1408) ([mpenick](https://github.com/mpenick))
- Fix \#1404: add/expose "byte\[\]" methods; remove legacy \(broken\) base64 handling workaround [\#1405](https://github.com/stargate/stargate/pull/1405) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1395](https://github.com/stargate/stargate/pull/1395) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.42](https://github.com/stargate/stargate/tree/v1.0.42) (2021-11-04)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.41...v1.0.42)

**Fixed bugs:**

- GraphQL API doesn't support inserting null values for some types [\#1347](https://github.com/stargate/stargate/issues/1347)

**Merged pull requests:**

- Bumping version for next release [\#1391](https://github.com/stargate/stargate/pull/1391) ([github-actions[bot]](https://github.com/apps/github-actions))
- Remove OSGI references from integration tests [\#1390](https://github.com/stargate/stargate/pull/1390) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.41](https://github.com/stargate/stargate/tree/v1.0.41) (2021-11-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.40...v1.0.41)

**Fixed bugs:**

- GraphQL API doesn't support inserting null values for some types [\#1347](https://github.com/stargate/stargate/issues/1347)

**Closed issues:**

- Incorrect documented return type for `RowsResource.getAllRows\(\)` [\#1378](https://github.com/stargate/stargate/issues/1378)
- Incorrect tests in `RestApiv2Test`, "createTableMissingClustering\(\)", "createTableWithNullOptions\(\)" [\#1376](https://github.com/stargate/stargate/issues/1376)
- Add read-me for the `metric-jersey` module [\#1312](https://github.com/stargate/stargate/issues/1312)

**Merged pull requests:**

- Add schema agreement check to persistence connection [\#1389](https://github.com/stargate/stargate/pull/1389) ([mpenick](https://github.com/mpenick))
- Remove the unpack\(\) call from gRPC docs [\#1387](https://github.com/stargate/stargate/pull/1387) ([tomekl007](https://github.com/tomekl007))
- GraphQL CQL-first: handle null values for complex types \(fixes \#1347\) [\#1380](https://github.com/stargate/stargate/pull/1380) ([olim7t](https://github.com/olim7t))
- Fix \#1378: change Swagger annotations to indicate correct response type [\#1379](https://github.com/stargate/stargate/pull/1379) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1376 \(2 tests using wrong response types\) [\#1377](https://github.com/stargate/stargate/pull/1377) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove unused `Any` import from gRPC proto file [\#1375](https://github.com/stargate/stargate/pull/1375) ([mpenick](https://github.com/mpenick))
- Improve RestV2 tests for "getAllRows\(\)" end point [\#1374](https://github.com/stargate/stargate/pull/1374) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1373](https://github.com/stargate/stargate/pull/1373) ([github-actions[bot]](https://github.com/apps/github-actions))
- Return UNAVAILABLE for unhandled clients [\#1366](https://github.com/stargate/stargate/pull/1366) ([mpenick](https://github.com/mpenick))
- closes \#1312: metric-jersey final refactoring and README [\#1364](https://github.com/stargate/stargate/pull/1364) ([ivansenic](https://github.com/ivansenic))

## [v1.0.40](https://github.com/stargate/stargate/tree/v1.0.40) (2021-10-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.39...v1.0.40)

**Fixed bugs:**

- Astra declines correct value of keyspace if the keyspace name is given in the query parameters [\#1317](https://github.com/stargate/stargate/issues/1317)
- Error when creating a new UDF [\#1092](https://github.com/stargate/stargate/issues/1092)

**Closed issues:**

- Change `config-store-yaml` to use Caffeine cache instead of Guava [\#1255](https://github.com/stargate/stargate/issues/1255)
- Stargate isn't able to load schema if Solr is used on a table [\#606](https://github.com/stargate/stargate/issues/606)

**Merged pull requests:**

- Add one missing piece wrt \#1255 for config-store-yaml/Caffeine dependency [\#1371](https://github.com/stargate/stargate/pull/1371) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove `Payload` type from the gRPC API [\#1370](https://github.com/stargate/stargate/pull/1370) ([mpenick](https://github.com/mpenick))
- Support User-Defined Function creation \(fixes \#1092\) [\#1362](https://github.com/stargate/stargate/pull/1362) ([olim7t](https://github.com/olim7t))
- Fix \#1255: replace Guava cache with Caffeine in config-store-yaml [\#1361](https://github.com/stargate/stargate/pull/1361) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Adapt CreateIndexTest for backends that default to SAI [\#1360](https://github.com/stargate/stargate/pull/1360) ([olim7t](https://github.com/olim7t))
- Simplify "maven-surefire-plugin" setup by removing per-module config [\#1358](https://github.com/stargate/stargate/pull/1358) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add Jeff to codeowners [\#1355](https://github.com/stargate/stargate/pull/1355) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add empty implementation for DSE search indexes \(fixes \#606\) [\#1354](https://github.com/stargate/stargate/pull/1354) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#1353](https://github.com/stargate/stargate/pull/1353) ([github-actions[bot]](https://github.com/apps/github-actions))
- gRPC: decorate keyspace if provided in query parameters \(fixes \#1317\) [\#1351](https://github.com/stargate/stargate/pull/1351) ([olim7t](https://github.com/olim7t))
- Rest api test factoring wrt json handling [\#1350](https://github.com/stargate/stargate/pull/1350) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- GraphQL schema-first: also pre-check where conditions on SAI indexes [\#1344](https://github.com/stargate/stargate/pull/1344) ([olim7t](https://github.com/olim7t))
- Fix gRPC prepared cache invalidation [\#1335](https://github.com/stargate/stargate/pull/1335) ([mpenick](https://github.com/mpenick))
- add request counter metrics possibility [\#1311](https://github.com/stargate/stargate/pull/1311) ([ivansenic](https://github.com/ivansenic))

## [v1.0.39](https://github.com/stargate/stargate/tree/v1.0.39) (2021-10-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.38...v1.0.39)

**Closed issues:**

- Trim "authnz" \(auth api\) package by removing slf4j dependency [\#1342](https://github.com/stargate/stargate/issues/1342)
- "auth-api" refactoring to follow conventions \(from `restapi`\) [\#1339](https://github.com/stargate/stargate/issues/1339)
- UDT fields messed up on writes through gRPC [\#1329](https://github.com/stargate/stargate/issues/1329)

**Merged pull requests:**

- Sync some missing \(4.0.0-\>4.0.1 / 3.11.8-\>3.11.9\) changes from \#1337, renaming [\#1346](https://github.com/stargate/stargate/pull/1346) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1342: remove trivial usage, dependency to slf4j/logback, by `authnz` to shrink jar/bundle by 95% [\#1343](https://github.com/stargate/stargate/pull/1343) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Revisit PROXY protocol rules [\#1341](https://github.com/stargate/stargate/pull/1341) ([olim7t](https://github.com/olim7t))
- Fix \#1339: refactoring/renaming of "auth-api" classes, entities [\#1340](https://github.com/stargate/stargate/pull/1340) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add close event to `EventListenerWithChannelFilter` [\#1338](https://github.com/stargate/stargate/pull/1338) ([mpenick](https://github.com/mpenick))
- Update C\*4.0 to latest \(4.0.1\) and 3.11 to next \(3.11.9\) [\#1337](https://github.com/stargate/stargate/pull/1337) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update dse-core 6.8.13 -\> 6.8.16; driver to bit newer as well [\#1334](https://github.com/stargate/stargate/pull/1334) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Insert/Delete query optimization [\#1333](https://github.com/stargate/stargate/pull/1333) ([EricBorczuk](https://github.com/EricBorczuk))
- Enable GraphQL schema-first by default [\#1332](https://github.com/stargate/stargate/pull/1332) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#1331](https://github.com/stargate/stargate/pull/1331) ([github-actions[bot]](https://github.com/apps/github-actions))
- Encode Udt field values in the correct order [\#1330](https://github.com/stargate/stargate/pull/1330) ([pkolaczk](https://github.com/pkolaczk))
- Enforce DSE guardrails when using external auth [\#1304](https://github.com/stargate/stargate/pull/1304) ([olim7t](https://github.com/olim7t))

## [v1.0.38](https://github.com/stargate/stargate/tree/v1.0.38) (2021-10-12)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.37...v1.0.38)

**Closed issues:**

- Make Duzzt-dependency `provided` to avoid adding processor classes for runtime [\#1327](https://github.com/stargate/stargate/issues/1327)
- Rename `restapi` type `Error` as `ApiError` \(for later extraction to shared package\) [\#1322](https://github.com/stargate/stargate/issues/1322)
- \(restapi\) Isolate more of REST API pieces under new `io.stargate.web.restapi` [\#1318](https://github.com/stargate/stargate/issues/1318)

**Merged pull requests:**

- Fix \#1327: make `duzzt` dependency "provided" \(6 fewer bundled jars\) [\#1328](https://github.com/stargate/stargate/pull/1328) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1326](https://github.com/stargate/stargate/pull/1326) ([github-actions[bot]](https://github.com/apps/github-actions))
- Another tiny refactoring for \#1318: make `Datacenter` a static inner class [\#1325](https://github.com/stargate/stargate/pull/1325) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1322: rename `Error` -\> `ApiError` [\#1323](https://github.com/stargate/stargate/pull/1323) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add ability to close/reject persistent connections [\#1315](https://github.com/stargate/stargate/pull/1315) ([mpenick](https://github.com/mpenick))

## [v1.0.37](https://github.com/stargate/stargate/tree/v1.0.37) (2021-10-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.36...v1.0.37)

**Closed issues:**

- Split, rename existing `Db` factory into `RestDBFactory`, `DocDBFactory` [\#1309](https://github.com/stargate/stargate/issues/1309)

**Merged pull requests:**

- Part 2 of \#1318: combine classes into related packages [\#1320](https://github.com/stargate/stargate/pull/1320) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- First part of \#1318: move REST API resources under ".../web/restapi/restapi/resources" [\#1319](https://github.com/stargate/stargate/pull/1319) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Second part of \#1309 \(and bit more\), further separating out "real" REST from Doc API [\#1316](https://github.com/stargate/stargate/pull/1316) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1314](https://github.com/stargate/stargate/pull/1314) ([github-actions[bot]](https://github.com/apps/github-actions))
- Compile path call is removed, Don't recompile already compiled regexes, and no longer calculate allColumnNames\(\) on every call [\#1307](https://github.com/stargate/stargate/pull/1307) ([EricBorczuk](https://github.com/EricBorczuk))
- Return private ip for peers if client connects over private/internal network [\#1180](https://github.com/stargate/stargate/pull/1180) ([olim7t](https://github.com/olim7t))

## [v1.0.36](https://github.com/stargate/stargate/tree/v1.0.36) (2021-10-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.35...v1.0.36)

**Fixed bugs:**

- `QueryBuilderImpl` adds comment without preprocessing with StringCodes [\#1293](https://github.com/stargate/stargate/issues/1293)
- `json-schema` Endpoint: GET request returns 400 on a collection with a schema [\#1291](https://github.com/stargate/stargate/issues/1291)
- `json-schema` Endpoint: Single Quotes in the "description" field of a JSON Schema property cause errors. [\#1290](https://github.com/stargate/stargate/issues/1290)

**Closed issues:**

- Improve encapsulation of `AuthenticatedDB` \(hide DataStore better\) [\#1301](https://github.com/stargate/stargate/issues/1301)
- Stargate fails to start because of too many open files [\#1286](https://github.com/stargate/stargate/issues/1286)

**Merged pull requests:**

- Fix \#1309: split `Db` into separate factories, rename data access types [\#1310](https://github.com/stargate/stargate/pull/1310) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update README.md [\#1308](https://github.com/stargate/stargate/pull/1308) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add value conversion methods for complex types [\#1305](https://github.com/stargate/stargate/pull/1305) ([mpenick](https://github.com/mpenick))
- Fix \#1301: encapsulate `AuthenticatedDB` better wrt DataStore [\#1302](https://github.com/stargate/stargate/pull/1302) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Provide a flag to disable watching the bundles directory [\#1300](https://github.com/stargate/stargate/pull/1300) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#1299](https://github.com/stargate/stargate/pull/1299) ([github-actions[bot]](https://github.com/apps/github-actions))
- closes \#1291: correct response in case JSON schema does not exist for… [\#1297](https://github.com/stargate/stargate/pull/1297) ([ivansenic](https://github.com/ivansenic))

## [v1.0.35](https://github.com/stargate/stargate/tree/v1.0.35) (2021-10-01)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.34...v1.0.35)

**Fixed bugs:**

- Jersey Binder is not making HK2 injections as singletongs [\#1280](https://github.com/stargate/stargate/issues/1280)
- Catch OverloadedException in HTTP services and return 429 [\#1233](https://github.com/stargate/stargate/issues/1233)
- Not able to retrieve row via REST with uppercase column name [\#1208](https://github.com/stargate/stargate/issues/1208)

**Closed issues:**

- Refactoring of REST/Doc Api: clean separation of `AuthenticatedDB` vs `DocumentDB` [\#1287](https://github.com/stargate/stargate/issues/1287)
- Refactor `restapi` to separate legacy REST v1 endpoints from v2 ones [\#1284](https://github.com/stargate/stargate/issues/1284)
- Log JsonSurfer/Jackson parsing issues without stack trace [\#1276](https://github.com/stargate/stargate/issues/1276)
- Add handling for skipped tests \(via "assume" vs assert\) for `ExternalStorage` to avoid false errors [\#1273](https://github.com/stargate/stargate/issues/1273)
- Remove stack trace from logging of "known exceptions" in `RequestHandler`, `AuthnJwtService` [\#1271](https://github.com/stargate/stargate/issues/1271)
- Reduce CI test noise by filtering out DEBUG entries from output by `ExternalStorage` [\#1265](https://github.com/stargate/stargate/issues/1265)
- Add GROUP BY support to CQL-first GraphQL API [\#1170](https://github.com/stargate/stargate/issues/1170)
- Max depth should always be read from the configuration [\#1030](https://github.com/stargate/stargate/issues/1030)

**Merged pull requests:**

- closes \#1293: correctly quote comment in create and alter table queries [\#1294](https://github.com/stargate/stargate/pull/1294) ([ivansenic](https://github.com/ivansenic))
- closes \#1280: all resources as singletons  [\#1289](https://github.com/stargate/stargate/pull/1289) ([ivansenic](https://github.com/ivansenic))
- Fix \#1287: clean up division between `DocumentDB` and `AuthenticatedDB` [\#1288](https://github.com/stargate/stargate/pull/1288) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1284: rename v1 endpoint classes, Server-\>RestApiServer [\#1285](https://github.com/stargate/stargate/pull/1285) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- relates to \#1280: make resources and services singletons in Docs API [\#1283](https://github.com/stargate/stargate/pull/1283) ([ivansenic](https://github.com/ivansenic))
- Update DEV\_GUIDE.md [\#1278](https://github.com/stargate/stargate/pull/1278) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#1276: handle Jackson-originating parsing fails more gracefully [\#1277](https://github.com/stargate/stargate/pull/1277) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1273: add explicit handling of Assume-sourced exceptions for Integ tests [\#1275](https://github.com/stargate/stargate/pull/1275) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1271: remove inclusion of stack trace for logging [\#1272](https://github.com/stargate/stargate/pull/1272) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1269](https://github.com/stargate/stargate/pull/1269) ([github-actions[bot]](https://github.com/apps/github-actions))
- Remove DEBUG log entries from "ExternalStorage" buffering \(fix \#1265\) [\#1268](https://github.com/stargate/stargate/pull/1268) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- eliminating conversion of column names to lower case [\#1262](https://github.com/stargate/stargate/pull/1262) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Second part to fully resolve \#1229: convert grpc module too [\#1260](https://github.com/stargate/stargate/pull/1260) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- shredJson\(\) refactor and testing [\#1259](https://github.com/stargate/stargate/pull/1259) ([EricBorczuk](https://github.com/EricBorczuk))
- gRPC tuning: Use direct executor instead of separate threadpool [\#1258](https://github.com/stargate/stargate/pull/1258) ([mpenick](https://github.com/mpenick))
-  GraphQL CQL-first: add groupBy \(fixes \#1170\) [\#1257](https://github.com/stargate/stargate/pull/1257) ([olim7t](https://github.com/olim7t))
- Report OverloadedException as a 429 in HTTP APIs \(fixes \#1233\) [\#1238](https://github.com/stargate/stargate/pull/1238) ([olim7t](https://github.com/olim7t))

## [v1.0.34](https://github.com/stargate/stargate/tree/v1.0.34) (2021-09-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.33...v1.0.34)

**Merged pull requests:**

- Headers needed to correctly validate token in all envs [\#1267](https://github.com/stargate/stargate/pull/1267) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#1266](https://github.com/stargate/stargate/pull/1266) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.33](https://github.com/stargate/stargate/tree/v1.0.33) (2021-09-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.32...v1.0.33)

**Fixed bugs:**

- fixed Cassandra predicate when searching docs with a boolean value [\#1212](https://github.com/stargate/stargate/pull/1212) ([ivansenic](https://github.com/ivansenic))

**Closed issues:**

- Reduce noise from CI build wrt "gsutil rsync" [\#1263](https://github.com/stargate/stargate/issues/1263)
- Change representation of `Uuid` type to use `bytes`  [\#1248](https://github.com/stargate/stargate/issues/1248)
- Readiness check URI is truncated to include only the first query parameter [\#1243](https://github.com/stargate/stargate/issues/1243)
- Make fragments of Document and Sub-Document available using WHERE query parameter [\#1242](https://github.com/stargate/stargate/issues/1242)
- Remove usage of `com.datastax.oss:java-driver-shaded-guava` \(com.datastax.oss.driver.shaded.guava.\*\) [\#1237](https://github.com/stargate/stargate/issues/1237)
- Reduce noise by `MeterRegistryConfiguration` validation wrt invalid Double values [\#1231](https://github.com/stargate/stargate/issues/1231)
- Replace direct Guava usage by Stargate \(use shaded everywhere applicable\) to reduce Guava conflict w/ Cassandra backend [\#1229](https://github.com/stargate/stargate/issues/1229)
- Create Docs API technical README.md [\#1228](https://github.com/stargate/stargate/issues/1228)
- Replace "jsurfer-gson" with "jsurfer-jackson" \(version 1.6.2\) [\#1227](https://github.com/stargate/stargate/issues/1227)
- Trying to read a document that is an array results in error [\#1224](https://github.com/stargate/stargate/issues/1224)
- Use health-check provide via `BaseActivator` in the graphql module, instead of custom mechanism [\#1221](https://github.com/stargate/stargate/issues/1221)
- Add health checks to grpc service [\#1219](https://github.com/stargate/stargate/issues/1219)
- rename: `io.stargate.grpc.service.Service` to `io.stargate.grpc.service.GrpcService` [\#1215](https://github.com/stargate/stargate/issues/1215)
- Small naming issues, use of deprecated options in CQL module [\#1210](https://github.com/stargate/stargate/issues/1210)
- Infer and propagate information about indepotency of gRPC queries [\#1196](https://github.com/stargate/stargate/issues/1196)
- Grow document search page size exponentially  [\#1183](https://github.com/stargate/stargate/issues/1183)
- gRPC: Waiting for schema agreement should be asynchronous  [\#1145](https://github.com/stargate/stargate/issues/1145)
- Add ability to "push" to an array path in a document [\#854](https://github.com/stargate/stargate/issues/854)
- Use direct guava artifacts instead of com.datastax.oss.driver.shaded.guava [\#814](https://github.com/stargate/stargate/issues/814)

**Merged pull requests:**

- \(WIP\) Fix \#1263: add "-q" option for `gsutil` to remove progress bar noise. [\#1264](https://github.com/stargate/stargate/pull/1264) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix a merge-induced formatting fail \(not caught be CI\) [\#1261](https://github.com/stargate/stargate/pull/1261) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use `bytes` types to encode UUID types [\#1256](https://github.com/stargate/stargate/pull/1256) ([mpenick](https://github.com/mpenick))
- Remove use of unshaded guava \(fix \#1229\) [\#1253](https://github.com/stargate/stargate/pull/1253) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Dev guide macos [\#1252](https://github.com/stargate/stargate/pull/1252) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- CQL: Expose backpressure options and set sensible defaults [\#1250](https://github.com/stargate/stargate/pull/1250) ([olim7t](https://github.com/olim7t))
- Minor renaming to improve readability of results \(Cass 3.11 vs 4.0 IT\) [\#1246](https://github.com/stargate/stargate/pull/1246) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update REST commands [\#1245](https://github.com/stargate/stargate/pull/1245) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add document API developer docs [\#1244](https://github.com/stargate/stargate/pull/1244) ([EricBorczuk](https://github.com/EricBorczuk))
- Add a gRPC interceptor that caches `Connection` [\#1241](https://github.com/stargate/stargate/pull/1241) ([mpenick](https://github.com/mpenick))
- Add new endpoints for "built-in" functions, add two built in funcs $push and $pop [\#1240](https://github.com/stargate/stargate/pull/1240) ([EricBorczuk](https://github.com/EricBorczuk))
- use cache in AuthnTableBasedService [\#1239](https://github.com/stargate/stargate/pull/1239) ([tomekl007](https://github.com/tomekl007))
- Reference example for gRPC java client [\#1236](https://github.com/stargate/stargate/pull/1236) ([tomekl007](https://github.com/tomekl007))
- Fix \#1227: replace gson-backed j\[son\]surfer with jackson-backed one [\#1234](https://github.com/stargate/stargate/pull/1234) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1231: improve WARN message for `MeterRegistryConfiguration` wrt invalid config value [\#1232](https://github.com/stargate/stargate/pull/1232) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Arrays can now be read in every case, even at the root of the document [\#1225](https://github.com/stargate/stargate/pull/1225) ([EricBorczuk](https://github.com/EricBorczuk))
- use health-check from `BaseActivator` for graphQL module [\#1222](https://github.com/stargate/stargate/pull/1222) ([tomekl007](https://github.com/tomekl007))
- closes \#1183: exponential storage page size and appropriate sizes in all resolvers and fetchers [\#1218](https://github.com/stargate/stargate/pull/1218) ([ivansenic](https://github.com/ivansenic))
- document gRPC deadlines [\#1217](https://github.com/stargate/stargate/pull/1217) ([tomekl007](https://github.com/tomekl007))
- rename Service to GrpcService [\#1216](https://github.com/stargate/stargate/pull/1216) ([tomekl007](https://github.com/tomekl007))
- Fix for \#1210: fix bundle name of `cql` module, Server-\>CqlServer, javadoc fixes [\#1211](https://github.com/stargate/stargate/pull/1211) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add reference to c\*4.0 persistence module in readme \(no longer an extension\) [\#1209](https://github.com/stargate/stargate/pull/1209) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#1204](https://github.com/stargate/stargate/pull/1204) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL: Add developer docs [\#1202](https://github.com/stargate/stargate/pull/1202) ([olim7t](https://github.com/olim7t))
- Exclude azure-storage-blob dependency from DSE dependencies [\#1199](https://github.com/stargate/stargate/pull/1199) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Infer and propagate information about indepotency of gRPC queries [\#1194](https://github.com/stargate/stargate/pull/1194) ([tomekl007](https://github.com/tomekl007))
- Retries for gRPC [\#1190](https://github.com/stargate/stargate/pull/1190) ([tomekl007](https://github.com/tomekl007))
- gRPC: wait for schema agreement asynchronously \(fixes \#1145\) [\#1172](https://github.com/stargate/stargate/pull/1172) ([olim7t](https://github.com/olim7t))
- Add metrics support for gRPC [\#976](https://github.com/stargate/stargate/pull/976) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.32](https://github.com/stargate/stargate/tree/v1.0.32) (2021-08-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.31...v1.0.32)

**Fixed bugs:**

- Docs API should have it's own module in the http metric [\#1185](https://github.com/stargate/stargate/issues/1185)
- Combining paging state does not maintain the order of single states [\#1162](https://github.com/stargate/stargate/issues/1162)
- Catch JsonParseException and return 400 [\#1110](https://github.com/stargate/stargate/issues/1110)

**Closed issues:**

- Remove `json-simple` library dependency \(unused\) [\#1178](https://github.com/stargate/stargate/issues/1178)
- High latency observed for \[201 POST\] /v1/keyspaces/{keyspaceName}/tables  [\#1164](https://github.com/stargate/stargate/issues/1164)
- gRPC: Remove `page\_size` from `ResultSet` type [\#1161](https://github.com/stargate/stargate/issues/1161)
- gRPC: set a default serial consistency level [\#1157](https://github.com/stargate/stargate/issues/1157)
- gRPC: use a default page size [\#1156](https://github.com/stargate/stargate/issues/1156)
- Possible lock in the reactive document service RX chain [\#1151](https://github.com/stargate/stargate/issues/1151)
- gRPC: refactor main service class to simplify control flow [\#1148](https://github.com/stargate/stargate/issues/1148)
- Flaky metrics test makes CI fails [\#1143](https://github.com/stargate/stargate/issues/1143)
- GraphQL schema-first: support federated tracing [\#1113](https://github.com/stargate/stargate/issues/1113)
- Support `$not` for the collection search [\#1071](https://github.com/stargate/stargate/issues/1071)
- Support `$or` document search [\#1063](https://github.com/stargate/stargate/issues/1063)
- Support $exists with false [\#1027](https://github.com/stargate/stargate/issues/1027)
- Failed to open hints directory [\#991](https://github.com/stargate/stargate/issues/991)
- Add documentation to the `.proto` files. [\#990](https://github.com/stargate/stargate/issues/990)
- gRPC calls could return schema objects for DDL queries [\#929](https://github.com/stargate/stargate/issues/929)

**Merged pull requests:**

- Exclude azure-storage-blob dependency from DSE dependencies [\#1199](https://github.com/stargate/stargate/pull/1199) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Move `grpc` into the `default` and `dse` profiles [\#1198](https://github.com/stargate/stargate/pull/1198) ([mpenick](https://github.com/mpenick))
- Update CODEOWNERS [\#1192](https://github.com/stargate/stargate/pull/1192) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Catch JsonProcessingException and return 400 status [\#1189](https://github.com/stargate/stargate/pull/1189) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Adding a PR template to the repo [\#1187](https://github.com/stargate/stargate/pull/1187) ([dougwettlaufer](https://github.com/dougwettlaufer))
- closes \#1185: docsapi to have it's own module in the metrics [\#1186](https://github.com/stargate/stargate/pull/1186) ([ivansenic](https://github.com/ivansenic))
- Fix \#1178 by removing unused/unnecessary dep to `json-simple` from poms [\#1179](https://github.com/stargate/stargate/pull/1179) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- gRPC: propagate HTTP headers to the persistence layer [\#1176](https://github.com/stargate/stargate/pull/1176) ([olim7t](https://github.com/olim7t))
- gRPC: set default consistency levels in the service \(fixes \#1157\) [\#1175](https://github.com/stargate/stargate/pull/1175) ([olim7t](https://github.com/olim7t))
- gRPC: use a default page size \(fixes \#1156\) [\#1174](https://github.com/stargate/stargate/pull/1174) ([olim7t](https://github.com/olim7t))
- gRPC: deprecate ResultSet.page\_size \(fixes \#1161\) [\#1173](https://github.com/stargate/stargate/pull/1173) ([olim7t](https://github.com/olim7t))
- document `$not` in Swagger [\#1171](https://github.com/stargate/stargate/pull/1171) ([ivansenic](https://github.com/ivansenic))
- Increased default size for document search to 3 [\#1166](https://github.com/stargate/stargate/pull/1166) ([EricBorczuk](https://github.com/EricBorczuk))
- Add default Docs API page sizes to Swagger descriptions [\#1163](https://github.com/stargate/stargate/pull/1163) ([dimas-b](https://github.com/dimas-b))
- Support more complex types grpc [\#1160](https://github.com/stargate/stargate/pull/1160) ([tomekl007](https://github.com/tomekl007))
- relates to \#1151: switch away from the core thread in the RX [\#1159](https://github.com/stargate/stargate/pull/1159) ([ivansenic](https://github.com/ivansenic))
- Support $not operators in Docs API WHERE clauses [\#1158](https://github.com/stargate/stargate/pull/1158) ([dimas-b](https://github.com/dimas-b))
- gRPC: expose schema change data in responses \(fixes \#929\) [\#1154](https://github.com/stargate/stargate/pull/1154) ([olim7t](https://github.com/olim7t))
- Do not block Rx operations when deleting "dead leaves" in Docs API [\#1153](https://github.com/stargate/stargate/pull/1153) ([dimas-b](https://github.com/dimas-b))
- gRPC: refactor Service class to simplify control flow \(fixes \#1148\) [\#1152](https://github.com/stargate/stargate/pull/1152) ([olim7t](https://github.com/olim7t))
- closes \#1027: support `$exists`  with false [\#1150](https://github.com/stargate/stargate/pull/1150) ([ivansenic](https://github.com/ivansenic))
- closes \#1143: added Awaitility to the MetricsTest [\#1149](https://github.com/stargate/stargate/pull/1149) ([ivansenic](https://github.com/ivansenic))
- gRPC: document proto files \(fixes \#990\) [\#1147](https://github.com/stargate/stargate/pull/1147) ([olim7t](https://github.com/olim7t))
- Add $and, $or to Swagger descriptions [\#1146](https://github.com/stargate/stargate/pull/1146) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#1144](https://github.com/stargate/stargate/pull/1144) ([github-actions[bot]](https://github.com/apps/github-actions))
- Improve gRPC java client [\#1140](https://github.com/stargate/stargate/pull/1140) ([tomekl007](https://github.com/tomekl007))
- Use Cassandra 4.0.0 GA [\#1139](https://github.com/stargate/stargate/pull/1139) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add $selectivity to Swagger descriptions [\#1138](https://github.com/stargate/stargate/pull/1138) ([dimas-b](https://github.com/dimas-b))
- closes \#1063: full support for $or queries in the docs api [\#1137](https://github.com/stargate/stargate/pull/1137) ([ivansenic](https://github.com/ivansenic))
- enable grpc in CI [\#1132](https://github.com/stargate/stargate/pull/1132) ([tomekl007](https://github.com/tomekl007))
- gRPC: authorize requests [\#1131](https://github.com/stargate/stargate/pull/1131) ([olim7t](https://github.com/olim7t))
- GraphQL schema-first: support federated tracing [\#1124](https://github.com/stargate/stargate/pull/1124) ([olim7t](https://github.com/olim7t))
- Encode special characters and decode them on read [\#1105](https://github.com/stargate/stargate/pull/1105) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.31](https://github.com/stargate/stargate/tree/v1.0.31) (2021-07-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.30...v1.0.31)

**Fixed bugs:**

- APIs should not return HTML content [\#1117](https://github.com/stargate/stargate/issues/1117)

**Closed issues:**

- GraphQL schema-first: allow entity field in delete response payload [\#1082](https://github.com/stargate/stargate/issues/1082)
- Enable user to explicitly specify filter priority in order to control the execution [\#1072](https://github.com/stargate/stargate/issues/1072)
- Document search service to support in document search [\#1028](https://github.com/stargate/stargate/issues/1028)
- Validating data store should compare against externalQueryString [\#1026](https://github.com/stargate/stargate/issues/1026)
- Reduce complexity of DocumentService.searchRows [\#537](https://github.com/stargate/stargate/issues/537)

**Merged pull requests:**

- Use Cassandra 4.0.0 GA [\#1139](https://github.com/stargate/stargate/pull/1139) ([dougwettlaufer](https://github.com/dougwettlaufer))
- closes \#537: clean-up the DocumentService [\#1136](https://github.com/stargate/stargate/pull/1136) ([ivansenic](https://github.com/ivansenic))
- Add selectivity hints to Docs API query filters [\#1134](https://github.com/stargate/stargate/pull/1134) ([dimas-b](https://github.com/dimas-b))
- Update DEV\_GUIDE.md [\#1130](https://github.com/stargate/stargate/pull/1130) ([dougwettlaufer](https://github.com/dougwettlaufer))
- fix ordering of fields in query.proto `Response` [\#1127](https://github.com/stargate/stargate/pull/1127) ([tomekl007](https://github.com/tomekl007))
- Fix prepared statement handling in ValidatingDataStore [\#1125](https://github.com/stargate/stargate/pull/1125) ([dimas-b](https://github.com/dimas-b))
- Grpc fix response types for batches to support LWTs [\#1123](https://github.com/stargate/stargate/pull/1123) ([tomekl007](https://github.com/tomekl007))
- closes \#1117: never return html code as the API response [\#1122](https://github.com/stargate/stargate/pull/1122) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next release [\#1120](https://github.com/stargate/stargate/pull/1120) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL schema-first: allow entity field in delete response payload [\#1118](https://github.com/stargate/stargate/pull/1118) ([olim7t](https://github.com/olim7t))
- relates to \#1028: glob wildcard to match both field and array item [\#1116](https://github.com/stargate/stargate/pull/1116) ([ivansenic](https://github.com/ivansenic))
- closes \#1028: refactored in-document search [\#1080](https://github.com/stargate/stargate/pull/1080) ([ivansenic](https://github.com/ivansenic))
- Include trace table content in the gRPC message [\#1077](https://github.com/stargate/stargate/pull/1077) ([tomekl007](https://github.com/tomekl007))
- Added a section for Mac developers to install the correct JDK [\#904](https://github.com/stargate/stargate/pull/904) ([jdavies](https://github.com/jdavies))

## [v1.0.30](https://github.com/stargate/stargate/tree/v1.0.30) (2021-07-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.29...v1.0.30)

**Fixed bugs:**

- RawDocument reports to have next if not all rows can be fetched in a single query [\#1090](https://github.com/stargate/stargate/issues/1090)
- Comparing filter operations tests wrongly [\#1088](https://github.com/stargate/stargate/issues/1088)
- Searching of documents must populate them without the query limit [\#1087](https://github.com/stargate/stargate/issues/1087)
- Searching collection can fail due to the race condition in JavaRx [\#1076](https://github.com/stargate/stargate/issues/1076)
- Bad JSON results in the response code 500 [\#1064](https://github.com/stargate/stargate/issues/1064)

**Closed issues:**

- GraphQL schema-first: improve `schema` response when no custom schema is deployed [\#1103](https://github.com/stargate/stargate/issues/1103)
- OpenAPI specs don't return typed responses in any API with a "raw" parameter [\#1101](https://github.com/stargate/stargate/issues/1101)
- BaseDocumentApiV2Test should not run as a standalone test in CI [\#1094](https://github.com/stargate/stargate/issues/1094)
- Move persistence-cassandra-4.0 to trunk branch until there is a GA release [\#1085](https://github.com/stargate/stargate/issues/1085)
- GraphQL CQL-first: bulk insert does not handle conditional batch [\#1068](https://github.com/stargate/stargate/issues/1068)
- GraphQL CQL-first: batched mutations don't handle LWTs correctly [\#1067](https://github.com/stargate/stargate/issues/1067)
- gRPC's prepared query cache gets out of sync with persistence's query cache [\#1065](https://github.com/stargate/stargate/issues/1065)
- Docs API getCollections should only return collections [\#1052](https://github.com/stargate/stargate/issues/1052)
- Support parsing of $or and $and conditions [\#1031](https://github.com/stargate/stargate/issues/1031)
- Frequent Codacy errors in CI: No content to map due to end-of-input [\#686](https://github.com/stargate/stargate/issues/686)

**Merged pull requests:**

- Allow CQL to be bound to more than one port [\#1115](https://github.com/stargate/stargate/pull/1115) ([mpenick](https://github.com/mpenick))
- Adding Tatu [\#1109](https://github.com/stargate/stargate/pull/1109) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix invalid \(e.g `0.0.0.0`\) `rpc\_address` in `system.local` [\#1108](https://github.com/stargate/stargate/pull/1108) ([mpenick](https://github.com/mpenick))
- GraphQL schema-first: allow schema fetch operation to return null [\#1106](https://github.com/stargate/stargate/pull/1106) ([olim7t](https://github.com/olim7t))
- Update openapi spec annotations for specific return types rather than generic responsewrapper [\#1102](https://github.com/stargate/stargate/pull/1102) ([gconaty](https://github.com/gconaty))
- Add responseContainer to getCollections swagger annotation [\#1100](https://github.com/stargate/stargate/pull/1100) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Throw a user error in case of value type mismatch in Document filters [\#1099](https://github.com/stargate/stargate/pull/1099) ([dimas-b](https://github.com/dimas-b))
- Make BaseDocumentApiV2Test abstract [\#1095](https://github.com/stargate/stargate/pull/1095) ([dimas-b](https://github.com/dimas-b))
- Add restriction on tables that aren't docs collections [\#1093](https://github.com/stargate/stargate/pull/1093) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1088: fixed comparing filters returning opposite results [\#1089](https://github.com/stargate/stargate/pull/1089) ([ivansenic](https://github.com/ivansenic))
- closes \#1085: cassandra-4 move driver to 4.0-rc2 [\#1086](https://github.com/stargate/stargate/pull/1086) ([ivansenic](https://github.com/ivansenic))
- closes \#1076: not using withLatestFrom that can swallow rows [\#1084](https://github.com/stargate/stargate/pull/1084) ([ivansenic](https://github.com/ivansenic))
- Add `@atomic` directive [\#1083](https://github.com/stargate/stargate/pull/1083) ([olim7t](https://github.com/olim7t))
- Add handling/testing for 400's on malformed JSON in requests [\#1079](https://github.com/stargate/stargate/pull/1079) ([EricBorczuk](https://github.com/EricBorczuk))
- GraphQL CQL-first: Fix handling of conditional batches [\#1078](https://github.com/stargate/stargate/pull/1078) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#1075](https://github.com/stargate/stargate/pull/1075) ([github-actions[bot]](https://github.com/apps/github-actions))
- Retry queries in grpc on PreparedQueryNotFoundException [\#1066](https://github.com/stargate/stargate/pull/1066) ([dougwettlaufer](https://github.com/dougwettlaufer))
- closes \#1031: support parsing of the $or and $and expressions [\#1061](https://github.com/stargate/stargate/pull/1061) ([ivansenic](https://github.com/ivansenic))

## [v1.0.29](https://github.com/stargate/stargate/tree/v1.0.29) (2021-06-29)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.28...v1.0.29)

**Fixed bugs:**

- Unable to create materialized view in Stargate 1.0.28 [\#1073](https://github.com/stargate/stargate/issues/1073)

**Closed issues:**

- GraphQL CQL-first: `@atomic` bulk insert should generate a CQL batch [\#1069](https://github.com/stargate/stargate/issues/1069)
- Refactor in-document get/search code to be aligned with DocumentSearchService [\#1059](https://github.com/stargate/stargate/issues/1059)

**Merged pull requests:**

- Support Materialized View comments [\#1074](https://github.com/stargate/stargate/pull/1074) ([dimas-b](https://github.com/dimas-b))
- GraphQL CQL-first: generate batch for atomic bulk insert [\#1070](https://github.com/stargate/stargate/pull/1070) ([olim7t](https://github.com/olim7t))
- gRPC tracing [\#1062](https://github.com/stargate/stargate/pull/1062) ([tomekl007](https://github.com/tomekl007))
- Bumping version for next release [\#1060](https://github.com/stargate/stargate/pull/1060) ([github-actions[bot]](https://github.com/apps/github-actions))
- Get the user from the original clientState when cloning [\#1053](https://github.com/stargate/stargate/pull/1053) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add endpoint to write multiple docs [\#1043](https://github.com/stargate/stargate/pull/1043) ([EricBorczuk](https://github.com/EricBorczuk))
- Support bulk insert [\#1042](https://github.com/stargate/stargate/pull/1042) ([tomekl007](https://github.com/tomekl007))

## [v1.0.28](https://github.com/stargate/stargate/tree/v1.0.28) (2021-06-24)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.27...v1.0.28)

**Implemented enhancements:**

- Add config option to disable graphql playground [\#791](https://github.com/stargate/stargate/issues/791)

**Fixed bugs:**

- Document search with path segment and $in returns wrong results [\#1049](https://github.com/stargate/stargate/issues/1049)
- Page-size not working in subdocuments if request not contains "where" query parameter [\#1040](https://github.com/stargate/stargate/issues/1040)
- Incorrect pageState when get subdocument \(AstraDb\) [\#1039](https://github.com/stargate/stargate/issues/1039)

**Closed issues:**

- Add possibility to ingest multiple rows in one call using the REST API [\#1056](https://github.com/stargate/stargate/issues/1056)
- Support TTL on the REST API [\#1055](https://github.com/stargate/stargate/issues/1055)
- Using "fields" without "where" [\#1046](https://github.com/stargate/stargate/issues/1046)
- GraphQL schema-first: fix type checks for conditions [\#1013](https://github.com/stargate/stargate/issues/1013)
- Gracefully handle missing default keyspace [\#1007](https://github.com/stargate/stargate/issues/1007)
- GraphQL CQL-first: support counter increments [\#986](https://github.com/stargate/stargate/issues/986)
- Add support for UDTs to gRPC [\#979](https://github.com/stargate/stargate/issues/979)
- Improve Document API parameter validation [\#947](https://github.com/stargate/stargate/issues/947)
- Adding per-keyspace and percentile Dropwizard metrics [\#944](https://github.com/stargate/stargate/issues/944)
- Update swagger to mention that page-state param should be URL encoded [\#867](https://github.com/stargate/stargate/issues/867)
- Improve collection search performance [\#680](https://github.com/stargate/stargate/issues/680)

**Merged pull requests:**

- relates to \#1024: extract row matching [\#1058](https://github.com/stargate/stargate/pull/1058) ([ivansenic](https://github.com/ivansenic))
- Allow fetching document fields without WHERE [\#1054](https://github.com/stargate/stargate/pull/1054) ([dimas-b](https://github.com/dimas-b))
- fixes \#1049: path segment search with in-memory filter fixed [\#1050](https://github.com/stargate/stargate/pull/1050) ([ivansenic](https://github.com/ivansenic))
- Support paginating over nested doc elements [\#1048](https://github.com/stargate/stargate/pull/1048) ([dimas-b](https://github.com/dimas-b))
- relates to \#1024: refactored doc search rows to node conversion [\#1047](https://github.com/stargate/stargate/pull/1047) ([ivansenic](https://github.com/ivansenic))
- Use URL-safe base64 alphabet for REST API paging state [\#1041](https://github.com/stargate/stargate/pull/1041) ([dimas-b](https://github.com/dimas-b))
- allow insert to return Boolean to have the unified API between insert and update [\#1038](https://github.com/stargate/stargate/pull/1038) ([tomekl007](https://github.com/tomekl007))
- closes \#947: document api validation finalized [\#1037](https://github.com/stargate/stargate/pull/1037) ([ivansenic](https://github.com/ivansenic))
- closes \#944: default http tag provider to return selected headers [\#1036](https://github.com/stargate/stargate/pull/1036) ([ivansenic](https://github.com/ivansenic))
- relates to \#944: property for collecting path params as tags in the h… [\#1035](https://github.com/stargate/stargate/pull/1035) ([ivansenic](https://github.com/ivansenic))
- GraphQL: use a single DataStore per HTTP request [\#1034](https://github.com/stargate/stargate/pull/1034) ([olim7t](https://github.com/olim7t))
- Merge feature/docsapi-search-service branch to master [\#1032](https://github.com/stargate/stargate/pull/1032) ([ivansenic](https://github.com/ivansenic))
- Minor refactor of JsonSchemaHandler [\#1023](https://github.com/stargate/stargate/pull/1023) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL schema-first: allow custom TTL for INSERT and UPDATE [\#1022](https://github.com/stargate/stargate/pull/1022) ([olim7t](https://github.com/olim7t))
- Add system property to disable graphql playground [\#1020](https://github.com/stargate/stargate/pull/1020) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL CQL-first: support counter increments \(fixes \#986\) [\#1019](https://github.com/stargate/stargate/pull/1019) ([olim7t](https://github.com/olim7t))
- Allow custom timestamp on operations [\#1018](https://github.com/stargate/stargate/pull/1018) ([tomekl007](https://github.com/tomekl007))
- extending INT tests for the docs api [\#1016](https://github.com/stargate/stargate/pull/1016) ([ivansenic](https://github.com/ivansenic))
- GraphQL schema-first: fix type checks for conditions \(fixes \#1013\) [\#1014](https://github.com/stargate/stargate/pull/1014) ([olim7t](https://github.com/olim7t))
- Rename property to enable GraphQL-first API [\#1012](https://github.com/stargate/stargate/pull/1012) ([olim7t](https://github.com/olim7t))
- Expose decorated partition keys for comparison at Stargate level [\#1010](https://github.com/stargate/stargate/pull/1010) ([dimas-b](https://github.com/dimas-b))
- Add comment to the Schema, for tables [\#1009](https://github.com/stargate/stargate/pull/1009) ([EricBorczuk](https://github.com/EricBorczuk))
- Gracefully handle missing default keyspace \(fixes \#1007\) [\#1008](https://github.com/stargate/stargate/pull/1008) ([olim7t](https://github.com/olim7t))
- Bump jetty-servlets from 9.4.40.v20210413 to 9.4.41.v20210516 [\#1006](https://github.com/stargate/stargate/pull/1006) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support incremental updates for UPDATE queries [\#1005](https://github.com/stargate/stargate/pull/1005) ([tomekl007](https://github.com/tomekl007))
- Define the CQL directives programmatically [\#1003](https://github.com/stargate/stargate/pull/1003) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#1000](https://github.com/stargate/stargate/pull/1000) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL schema-first: allow custom consistency levels [\#996](https://github.com/stargate/stargate/pull/996) ([olim7t](https://github.com/olim7t))
- Add support for user-defined types to gRPC [\#995](https://github.com/stargate/stargate/pull/995) ([mpenick](https://github.com/mpenick))
- Add resource to attach a JSON schema to a collection [\#994](https://github.com/stargate/stargate/pull/994) ([EricBorczuk](https://github.com/EricBorczuk))
- Allow the persistence base directory to be overridden [\#993](https://github.com/stargate/stargate/pull/993) ([mpenick](https://github.com/mpenick))

## [v1.0.27](https://github.com/stargate/stargate/tree/v1.0.27) (2021-06-08)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.26...v1.0.27)

**Closed issues:**

- Make stargate\_graphql replication options configurable [\#987](https://github.com/stargate/stargate/issues/987)
- Improve gRPC error handling [\#923](https://github.com/stargate/stargate/issues/923)

**Merged pull requests:**

- Avoid filtering events that already have a channel filter associated … [\#997](https://github.com/stargate/stargate/pull/997) ([tjake](https://github.com/tjake))
- Bumping version for next release [\#992](https://github.com/stargate/stargate/pull/992) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add system property to configure replication options of stargate\_graphql \(fixes \#987\) [\#989](https://github.com/stargate/stargate/pull/989) ([olim7t](https://github.com/olim7t))
- gRPC errors [\#978](https://github.com/stargate/stargate/pull/978) ([mpenick](https://github.com/mpenick))
- Allow IF conditions for UPDATE queries [\#975](https://github.com/stargate/stargate/pull/975) ([tomekl007](https://github.com/tomekl007))

## [v1.0.26](https://github.com/stargate/stargate/tree/v1.0.26) (2021-06-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.25...v1.0.26)

**Implemented enhancements:**

- Query Chaining for GraphQL API [\#154](https://github.com/stargate/stargate/issues/154)

**Fixed bugs:**

- Inconsistencies in C\* RequestFailureReason mapping for DSE [\#982](https://github.com/stargate/stargate/issues/982)
- Stargate does not load non-system keyspaces if it tries to throw an InvalidTypeException [\#645](https://github.com/stargate/stargate/issues/645)

**Closed issues:**

- Query plan analysis for combined and-queries [\#973](https://github.com/stargate/stargate/issues/973)
- Revisit `GraphqlCache.DmlGraphqlHolder` [\#959](https://github.com/stargate/stargate/issues/959)
- Add support for collections to gRPC [\#935](https://github.com/stargate/stargate/issues/935)
- Prevent `USE \<keyspace\>` queries over gRPC [\#928](https://github.com/stargate/stargate/issues/928)
- Support partition key level deletes with GraphQL API [\#583](https://github.com/stargate/stargate/issues/583)
- GraphQL: Document collections, batches and UDTs support [\#312](https://github.com/stargate/stargate/issues/312)

**Merged pull requests:**

- Avoid proactively fetching empty pages when one result set row is expected [\#988](https://github.com/stargate/stargate/pull/988) ([dimas-b](https://github.com/dimas-b))
- Add dependency to the legacy driver in OSS persistence modules [\#985](https://github.com/stargate/stargate/pull/985) ([olim7t](https://github.com/olim7t))
- Add RequestFailureReason enum values for DSE [\#983](https://github.com/stargate/stargate/pull/983) ([dimas-b](https://github.com/dimas-b))
- Add Docs API query profiling [\#980](https://github.com/stargate/stargate/pull/980) ([dimas-b](https://github.com/dimas-b))
- Fix NPE caused by `AuthenticationInterceptor` [\#977](https://github.com/stargate/stargate/pull/977) ([mpenick](https://github.com/mpenick))
- relates to \#944: config prop for the http request metrics percentiles [\#974](https://github.com/stargate/stargate/pull/974) ([ivansenic](https://github.com/ivansenic))
- GraphQL schema-first: check authorization for the DDL queries of a deploySchema [\#972](https://github.com/stargate/stargate/pull/972) ([olim7t](https://github.com/olim7t))
- GraphQL: simplify cache internals \(fixes \#959\) [\#971](https://github.com/stargate/stargate/pull/971) ([olim7t](https://github.com/olim7t))
- gRPC API cleanup [\#970](https://github.com/stargate/stargate/pull/970) ([mpenick](https://github.com/mpenick))
- Change to 404 on empty GET [\#969](https://github.com/stargate/stargate/pull/969) ([EricBorczuk](https://github.com/EricBorczuk))
- relates to \#947: added validations for NamespacesResource [\#968](https://github.com/stargate/stargate/pull/968) ([ivansenic](https://github.com/ivansenic))
- GrapqhQL-first update IT [\#967](https://github.com/stargate/stargate/pull/967) ([tomekl007](https://github.com/tomekl007))
- Reduce the number of predicates in nested CQL queries for Docs API filters [\#966](https://github.com/stargate/stargate/pull/966) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#965](https://github.com/stargate/stargate/pull/965) ([github-actions[bot]](https://github.com/apps/github-actions))
- releates to \#947: validation for the CollectionsResource [\#962](https://github.com/stargate/stargate/pull/962) ([ivansenic](https://github.com/ivansenic))
- GraphQL schema-first: don't return full stacktrace some parsing errors [\#961](https://github.com/stargate/stargate/pull/961) ([olim7t](https://github.com/olim7t))
- Remove duplicate integration test [\#960](https://github.com/stargate/stargate/pull/960) ([olim7t](https://github.com/olim7t))
- Custom if clause for delete [\#958](https://github.com/stargate/stargate/pull/958) ([tomekl007](https://github.com/tomekl007))
- Refactor docs api pagination [\#937](https://github.com/stargate/stargate/pull/937) ([dimas-b](https://github.com/dimas-b))

## [v1.0.25](https://github.com/stargate/stargate/tree/v1.0.25) (2021-05-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.24...v1.0.25)

**Implemented enhancements:**

- Add "CREATE TYPE" to REST API for creating UDTs [\#579](https://github.com/stargate/stargate/issues/579)

**Closed issues:**

- Pass all fetcher context via HttpAwareContext [\#945](https://github.com/stargate/stargate/issues/945)
- updateColumn should allow altering type [\#845](https://github.com/stargate/stargate/issues/845)
- JVM OnOutOfMemoryError configuration warning on startup [\#793](https://github.com/stargate/stargate/issues/793)
- With empty tables POST to /graphql/keyspace fails [\#769](https://github.com/stargate/stargate/issues/769)
- Restapi only uses last clustering expression in list [\#406](https://github.com/stargate/stargate/issues/406)
- Built-in C\* aggregation functions [\#130](https://github.com/stargate/stargate/issues/130)
- Add async writes [\#128](https://github.com/stargate/stargate/issues/128)

**Merged pull requests:**

- Update DSE persistence to 6.8.13 [\#964](https://github.com/stargate/stargate/pull/964) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL: pass all fetcher dependencies through environment.getContext\(\) \(fixes \#945\) [\#956](https://github.com/stargate/stargate/pull/956) ([olim7t](https://github.com/olim7t))
- Prevent "USE \<keyspace\>" requests [\#955](https://github.com/stargate/stargate/pull/955) ([mpenick](https://github.com/mpenick))
- Reset warnings in finally for dse [\#954](https://github.com/stargate/stargate/pull/954) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL schema-first: use user-scoped datastore to fetch schema files [\#953](https://github.com/stargate/stargate/pull/953) ([olim7t](https://github.com/olim7t))
- update schema cache from DeploySchemaFetcherBase [\#950](https://github.com/stargate/stargate/pull/950) ([tomekl007](https://github.com/tomekl007))
- fix the introspection query for an empty keyspace [\#949](https://github.com/stargate/stargate/pull/949) ([tomekl007](https://github.com/tomekl007))
- GraphQL schema-first: handle persistence backends that default to SAI indexes [\#946](https://github.com/stargate/stargate/pull/946) ([olim7t](https://github.com/olim7t))
- speed-up the BaseDocumentApiV2Test [\#943](https://github.com/stargate/stargate/pull/943) ([ivansenic](https://github.com/ivansenic))
- Added client info metric tag provider [\#941](https://github.com/stargate/stargate/pull/941) ([ivansenic](https://github.com/ivansenic))
- Add async writes [\#940](https://github.com/stargate/stargate/pull/940) ([tomekl007](https://github.com/tomekl007))
- Bump cassandra-all from 3.11.6 to 3.11.8 in /persistence-common [\#939](https://github.com/stargate/stargate/pull/939) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump cassandra-all from 3.11.6 to 3.11.8 in /persistence-cassandra-3.11 [\#938](https://github.com/stargate/stargate/pull/938) ([dependabot[bot]](https://github.com/apps/dependabot))
- gRPC: Collections [\#936](https://github.com/stargate/stargate/pull/936) ([mpenick](https://github.com/mpenick))
- Add checks for document table validity before querying [\#933](https://github.com/stargate/stargate/pull/933) ([EricBorczuk](https://github.com/EricBorczuk))
- gRPC: Add warnings [\#932](https://github.com/stargate/stargate/pull/932) ([mpenick](https://github.com/mpenick))
- Include gRPC stuff in release bump [\#927](https://github.com/stargate/stargate/pull/927) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#926](https://github.com/stargate/stargate/pull/926) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix the response code for getAllIndexes [\#924](https://github.com/stargate/stargate/pull/924) ([dougwettlaufer](https://github.com/dougwettlaufer))
- introduced error code for better handling of the responses from… [\#921](https://github.com/stargate/stargate/pull/921) ([ivansenic](https://github.com/ivansenic))
- generic constructor for ServiceAndProperties [\#920](https://github.com/stargate/stargate/pull/920) ([ivansenic](https://github.com/ivansenic))
- gRPC: Batches [\#918](https://github.com/stargate/stargate/pull/918) ([mpenick](https://github.com/mpenick))
- Split GraphQL legacy tests and limit usage of Apollo library [\#906](https://github.com/stargate/stargate/pull/906) ([olim7t](https://github.com/olim7t))
- Cql first build in aggregations [\#901](https://github.com/stargate/stargate/pull/901) ([tomekl007](https://github.com/tomekl007))
- Add Support for UserDefinedType in RestAPIv2  [\#895](https://github.com/stargate/stargate/pull/895) ([eribeiro](https://github.com/eribeiro))
- Start adding paging [\#885](https://github.com/stargate/stargate/pull/885) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.24](https://github.com/stargate/stargate/tree/v1.0.24) (2021-05-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.23...v1.0.24)

**Closed issues:**

- Add possibility to invoke C\* aggregation functions on `DataStore.queryBuilder\(\)` [\#907](https://github.com/stargate/stargate/issues/907)
- Operation timed out when using where on a set of 100.000 documents [\#905](https://github.com/stargate/stargate/issues/905)
- Question -  Stargate Support with WCF Or .Net Api [\#841](https://github.com/stargate/stargate/issues/841)
- Use the auth caching in docsapi in the remainder of restapi [\#551](https://github.com/stargate/stargate/issues/551)

**Merged pull requests:**

- Support custom paging positions within a partition [\#919](https://github.com/stargate/stargate/pull/919) ([dimas-b](https://github.com/dimas-b))
- Fix issue with last document not being registered in pagination [\#917](https://github.com/stargate/stargate/pull/917) ([EricBorczuk](https://github.com/EricBorczuk))
- gRPC: Add remaining primitive codecs [\#916](https://github.com/stargate/stargate/pull/916) ([mpenick](https://github.com/mpenick))
- Don't get all the candidates if you're only returning a few of them [\#914](https://github.com/stargate/stargate/pull/914) ([EricBorczuk](https://github.com/EricBorczuk))
- Add possibility to invoke C\* aggregation functions on DataStore.queryBuilder\(\) [\#913](https://github.com/stargate/stargate/pull/913) ([tomekl007](https://github.com/tomekl007))
- Bumping version for next release [\#912](https://github.com/stargate/stargate/pull/912) ([github-actions[bot]](https://github.com/apps/github-actions))
- explained more about debug running of the int tests [\#908](https://github.com/stargate/stargate/pull/908) ([ivansenic](https://github.com/ivansenic))
- Json Conversion Refactor: Part 2 [\#890](https://github.com/stargate/stargate/pull/890) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.23](https://github.com/stargate/stargate/tree/v1.0.23) (2021-04-29)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.22...v1.0.23)

**Fixed bugs:**

- Inserting row with data not matching schema returns 500 [\#718](https://github.com/stargate/stargate/issues/718)

**Closed issues:**

- Logging every REST API call [\#896](https://github.com/stargate/stargate/issues/896)
- Unable to use "$in" with timestamp field [\#887](https://github.com/stargate/stargate/issues/887)
- trying to add Stargate Auth in aspdot net core api [\#880](https://github.com/stargate/stargate/issues/880)
- Update row using GQL with `ifExists: true` response needs updates when the row doesn't actually get inserted/upserted [\#843](https://github.com/stargate/stargate/issues/843)
- Add a way to insert multiple objects at once [\#743](https://github.com/stargate/stargate/issues/743)
- Operation failed - received 0 responses and 1 failures: READ\_TOO\_MANY\_TOMBSTONES when using where= [\#711](https://github.com/stargate/stargate/issues/711)
- Searching over collection does not support getting a nested field, while searching within a document does [\#571](https://github.com/stargate/stargate/issues/571)
- Graphql schema first workflow [\#123](https://github.com/stargate/stargate/issues/123)

**Merged pull requests:**

- resolve possible request not flushed in metrics tests [\#909](https://github.com/stargate/stargate/pull/909) ([ivansenic](https://github.com/ivansenic))
- small updates for the metrics test [\#902](https://github.com/stargate/stargate/pull/902) ([ivansenic](https://github.com/ivansenic))
- Update readme with thanks [\#900](https://github.com/stargate/stargate/pull/900) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bring commons-io version into main pom.xml [\#898](https://github.com/stargate/stargate/pull/898) ([mpenick](https://github.com/mpenick))
- Handle where with  using types other than text fixes \#887 [\#888](https://github.com/stargate/stargate/pull/888) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Regenerate licenses-report.txt [\#882](https://github.com/stargate/stargate/pull/882) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Refactor JSON construction/conversion into its own service, with tests [\#879](https://github.com/stargate/stargate/pull/879) ([EricBorczuk](https://github.com/EricBorczuk))
- Upgrade jetty version because of vulnerability [\#876](https://github.com/stargate/stargate/pull/876) ([mpenick](https://github.com/mpenick))
- Fix silent deploy failures [\#875](https://github.com/stargate/stargate/pull/875) ([mpenick](https://github.com/mpenick))
- Add a way to insert multiple objects at once  [\#874](https://github.com/stargate/stargate/pull/874) ([tomekl007](https://github.com/tomekl007))
- Add support for nested fields in document search [\#873](https://github.com/stargate/stargate/pull/873) ([EricBorczuk](https://github.com/EricBorczuk))
- Use custom paging positions in Docs API [\#872](https://github.com/stargate/stargate/pull/872) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#871](https://github.com/stargate/stargate/pull/871) ([github-actions[bot]](https://github.com/apps/github-actions))
- fixes \#843: better responses for the mutation queries [\#857](https://github.com/stargate/stargate/pull/857) ([ivansenic](https://github.com/ivansenic))
- Support custom paging positions [\#855](https://github.com/stargate/stargate/pull/855) ([dimas-b](https://github.com/dimas-b))
- gRPC basic value handling [\#852](https://github.com/stargate/stargate/pull/852) ([mpenick](https://github.com/mpenick))
- Add GraphQL schema-first API \(fixes \#123\) [\#634](https://github.com/stargate/stargate/pull/634) ([olim7t](https://github.com/olim7t))

## [v1.0.22](https://github.com/stargate/stargate/tree/v1.0.22) (2021-04-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.21...v1.0.22)

**Fixed bugs:**

- `fields` parameter with a space in it causes server error in REST API v2 [\#865](https://github.com/stargate/stargate/issues/865)

**Closed issues:**

- Add ability to get all rows in REST v2 [\#728](https://github.com/stargate/stargate/issues/728)

**Merged pull requests:**

- Bumping version for next release [\#864](https://github.com/stargate/stargate/pull/864) ([github-actions[bot]](https://github.com/apps/github-actions))
- Added in Paginator object, use it everywhere [\#848](https://github.com/stargate/stargate/pull/848) ([EricBorczuk](https://github.com/EricBorczuk))
- Fixes \#728 - Add ability to get all rows in REST v2 [\#778](https://github.com/stargate/stargate/pull/778) ([eribeiro](https://github.com/eribeiro))

## [v1.0.21](https://github.com/stargate/stargate/tree/v1.0.21) (2021-04-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.20...v1.0.21)

**Merged pull requests:**

- Use project version for grpc-proto [\#863](https://github.com/stargate/stargate/pull/863) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#860](https://github.com/stargate/stargate/pull/860) ([github-actions[bot]](https://github.com/apps/github-actions))
- Added server http requests micrometer metric with custom tags [\#836](https://github.com/stargate/stargate/pull/836) ([ivansenic](https://github.com/ivansenic))

## [v1.0.20](https://github.com/stargate/stargate/tree/v1.0.20) (2021-04-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.19...v1.0.20)

**Merged pull requests:**

- Bumping version for next release [\#859](https://github.com/stargate/stargate/pull/859) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.19](https://github.com/stargate/stargate/tree/v1.0.19) (2021-04-15)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.18...v1.0.19)

**Closed issues:**

- Prototype Go client [\#850](https://github.com/stargate/stargate/issues/850)
- Add another example of retrieving a composite primary key [\#849](https://github.com/stargate/stargate/issues/849)
- CDC solution using the C\* as an outbox [\#846](https://github.com/stargate/stargate/issues/846)
- Support per partition limits in QueryBuilder [\#835](https://github.com/stargate/stargate/issues/835)
- `contains` and `containsEntry` don't seem to work when conjuncted in the REST API \(NOT documents\) [\#819](https://github.com/stargate/stargate/issues/819)
- Persistence API does not surface all index properties [\#803](https://github.com/stargate/stargate/issues/803)
- Publish integration tests as a normal jar with dependencies [\#774](https://github.com/stargate/stargate/issues/774)
- REST API for v2 create table fails when there is no "clusteringExpression" provided as part of the "tableOptions" [\#764](https://github.com/stargate/stargate/issues/764)
- REST v2: Allow complex JSON types as $eq arguments in where clause [\#639](https://github.com/stargate/stargate/issues/639)
- GraphQL: Make all data fetchers async [\#421](https://github.com/stargate/stargate/issues/421)
- Use prepared statements for GraphQL query executions [\#273](https://github.com/stargate/stargate/issues/273)

**Merged pull requests:**

- Include gRPC proto files in repo \(remove submodules\) [\#856](https://github.com/stargate/stargate/pull/856) ([mpenick](https://github.com/mpenick))
- Add basic authentication to gRPC [\#847](https://github.com/stargate/stargate/pull/847) ([mpenick](https://github.com/mpenick))
- Return 200 when updating a table with REST [\#844](https://github.com/stargate/stargate/pull/844) ([dougwettlaufer](https://github.com/dougwettlaufer))
- When creating a table with restapi default clustering to asc [\#840](https://github.com/stargate/stargate/pull/840) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add perPartitionLimit\(\) to QueryBuilder DSL [\#838](https://github.com/stargate/stargate/pull/838) ([dimas-b](https://github.com/dimas-b))
- Fix \#819 - contains and containsEntry with AND [\#837](https://github.com/stargate/stargate/pull/837) ([eribeiro](https://github.com/eribeiro))
- Package integration tests as plain jar [\#833](https://github.com/stargate/stargate/pull/833) ([dimas-b](https://github.com/dimas-b))
- gRPC boilerplate [\#832](https://github.com/stargate/stargate/pull/832) ([mpenick](https://github.com/mpenick))
- Introduce micrometer, wrap Dropwizard metrics [\#827](https://github.com/stargate/stargate/pull/827) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next release [\#826](https://github.com/stargate/stargate/pull/826) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fixes \#803 - surface all index properties [\#813](https://github.com/stargate/stargate/pull/813) ([eribeiro](https://github.com/eribeiro))
- Add optimization for multiple filters in `where` for documents API [\#748](https://github.com/stargate/stargate/pull/748) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.18](https://github.com/stargate/stargate/tree/v1.0.18) (2021-04-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.17...v1.0.18)

**Closed issues:**

- Fix geotype warnings in GraphQL tests [\#817](https://github.com/stargate/stargate/issues/817)
- \[Document API\] Ability to initialize empty collection  [\#805](https://github.com/stargate/stargate/issues/805)
- Consider removing dynamic OSGi imports [\#730](https://github.com/stargate/stargate/issues/730)

**Merged pull requests:**

- Skip geo types in GraphQL scalars test \(fixes \#817\) [\#822](https://github.com/stargate/stargate/pull/822) ([olim7t](https://github.com/olim7t))
- Use logback for logging in Dropwizard.  [\#816](https://github.com/stargate/stargate/pull/816) ([tomekl007](https://github.com/tomekl007))
- Bumping version for next release [\#807](https://github.com/stargate/stargate/pull/807) ([github-actions[bot]](https://github.com/apps/github-actions))
- Remove dynamic OSGi imports [\#806](https://github.com/stargate/stargate/pull/806) ([dimas-b](https://github.com/dimas-b))
- Use ResourceKind in IndexesResource [\#795](https://github.com/stargate/stargate/pull/795) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.17](https://github.com/stargate/stargate/tree/v1.0.17) (2021-03-29)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.16...v1.0.17)

**Closed issues:**

- Better status codes for table not exist/table already exists errors [\#775](https://github.com/stargate/stargate/issues/775)

**Merged pull requests:**

- Do not fail health check when schema is in agreement with storage nodes [\#802](https://github.com/stargate/stargate/pull/802) ([dimas-b](https://github.com/dimas-b))
- Move the resetting of ExecutorLocals to the start of executeRequest [\#797](https://github.com/stargate/stargate/pull/797) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#796](https://github.com/stargate/stargate/pull/796) ([github-actions[bot]](https://github.com/apps/github-actions))
- fixes \#775: better status codes for missing tables in document api [\#785](https://github.com/stargate/stargate/pull/785) ([ivansenic](https://github.com/ivansenic))

## [v1.0.16](https://github.com/stargate/stargate/tree/v1.0.16) (2021-03-24)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.15...v1.0.16)

**Implemented enhancements:**

- Add "CREATE INDEX" to REST API [\#580](https://github.com/stargate/stargate/issues/580)

**Closed issues:**

- JVM OnOutOfMemoryError configuration warning on startup [\#793](https://github.com/stargate/stargate/issues/793)
- UserDefinedType Resource in the schema RestApi [\#790](https://github.com/stargate/stargate/issues/790)
- Restarting stargate immediately results in startup failure [\#782](https://github.com/stargate/stargate/issues/782)
- starting stargate from systemd unit file [\#776](https://github.com/stargate/stargate/issues/776)
- Create Release didn't happen but it didn't fail [\#773](https://github.com/stargate/stargate/issues/773)
- Bad request, use ALLOW FILTERING [\#772](https://github.com/stargate/stargate/issues/772)
- Allow filtering on REST endpoint [\#766](https://github.com/stargate/stargate/issues/766)
- Return Cache-Control and Expires headers on successful REST authorization call [\#762](https://github.com/stargate/stargate/issues/762)
- Error 500 returns HTML and not a JSON body [\#740](https://github.com/stargate/stargate/issues/740)
- exists only supports the value true [\#720](https://github.com/stargate/stargate/issues/720)
- Validate CQL access to storage nodes during readiness check [\#631](https://github.com/stargate/stargate/issues/631)

**Merged pull requests:**

- Use ResourceKind in IndexesResource [\#795](https://github.com/stargate/stargate/pull/795) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Read auth tokens at CL == LOCAL\_QUORUM [\#794](https://github.com/stargate/stargate/pull/794) ([dimas-b](https://github.com/dimas-b))
- change CL of StorageHealthChecker queries to LOCAL\_QUORUM [\#789](https://github.com/stargate/stargate/pull/789) ([tomekl007](https://github.com/tomekl007))
- Quick graphql readme cleanup [\#787](https://github.com/stargate/stargate/pull/787) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Reset ExecutorLocals in DSE persistence requests [\#786](https://github.com/stargate/stargate/pull/786) ([dimas-b](https://github.com/dimas-b))
- Bump testcontainers to 1.15.2 [\#784](https://github.com/stargate/stargate/pull/784) ([olim7t](https://github.com/olim7t))
- Add a new Resource type to schema authz [\#783](https://github.com/stargate/stargate/pull/783) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fixes \#580 - Allows index creation/dropping via the REST API [\#771](https://github.com/stargate/stargate/pull/771) ([eribeiro](https://github.com/eribeiro))
- Add Cache-Control header to auth-api responses - Fixes \#762 [\#767](https://github.com/stargate/stargate/pull/767) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#765](https://github.com/stargate/stargate/pull/765) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use user-scoped datastore to compute GraphQL schemas [\#761](https://github.com/stargate/stargate/pull/761) ([olim7t](https://github.com/olim7t))
- Validate CQL access to storage nodes during readiness and liveness checks [\#742](https://github.com/stargate/stargate/pull/742) ([tomekl007](https://github.com/tomekl007))
- Try to run gcb steps in parallel again [\#733](https://github.com/stargate/stargate/pull/733) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.15](https://github.com/stargate/stargate/tree/v1.0.15) (2021-03-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.14...v1.0.15)

**Fixed bugs:**

- io.stargate.web.service.WhereParser\#parseWhere throws NPE for bad column name [\#752](https://github.com/stargate/stargate/issues/752)

**Closed issues:**

- Ensure metric is reported for unexpected exceptions during auth [\#760](https://github.com/stargate/stargate/issues/760)
- Generate an auth token [\#746](https://github.com/stargate/stargate/issues/746)

**Merged pull requests:**

- Adding Ivan [\#763](https://github.com/stargate/stargate/pull/763) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fixes \#752 - NPE for bad column name in Where clause [\#757](https://github.com/stargate/stargate/pull/757) ([eribeiro](https://github.com/eribeiro))
- Add metric to cql for auth errors [\#756](https://github.com/stargate/stargate/pull/756) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#754](https://github.com/stargate/stargate/pull/754) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add request rate metric to CQL [\#753](https://github.com/stargate/stargate/pull/753) ([mpenick](https://github.com/mpenick))

## [v1.0.14](https://github.com/stargate/stargate/tree/v1.0.14) (2021-03-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.13...v1.0.14)

**Closed issues:**

- Upgrade graphql-java from 14.2 to 16.2 [\#736](https://github.com/stargate/stargate/issues/736)
- Clean up GraphQL authentication [\#732](https://github.com/stargate/stargate/issues/732)
- Inconsistent authentication error messages in REST API and DOC API [\#630](https://github.com/stargate/stargate/issues/630)

**Merged pull requests:**

- Support geospacial types [\#750](https://github.com/stargate/stargate/pull/750) ([dimas-b](https://github.com/dimas-b))
- Update CODEOWNERS [\#747](https://github.com/stargate/stargate/pull/747) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Allow tests to customize cassandra.yaml in storage nodes [\#744](https://github.com/stargate/stargate/pull/744) ([dimas-b](https://github.com/dimas-b))
- Clean up GraphQL authentication \(fixes \#732\) [\#738](https://github.com/stargate/stargate/pull/738) ([olim7t](https://github.com/olim7t))
- Reduce Netty buffer memory usage for CQL transport [\#737](https://github.com/stargate/stargate/pull/737) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#734](https://github.com/stargate/stargate/pull/734) ([github-actions[bot]](https://github.com/apps/github-actions))
- Improved error message when schema agreement fails, as currently it always logs the check took zero milliseconds [\#726](https://github.com/stargate/stargate/pull/726) ([maxtomassi](https://github.com/maxtomassi))
- set authenticator via system property [\#725](https://github.com/stargate/stargate/pull/725) ([tomekl007](https://github.com/tomekl007))
- Fix \#630 - Inconsistent authentication error messages [\#704](https://github.com/stargate/stargate/pull/704) ([eribeiro](https://github.com/eribeiro))

## [v1.0.13](https://github.com/stargate/stargate/tree/v1.0.13) (2021-03-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.12...v1.0.13)

**Fixed bugs:**

- Querying GraphQL API with `limit` fails [\#716](https://github.com/stargate/stargate/issues/716)
- Require authorization to read DML schemas [\#731](https://github.com/stargate/stargate/pull/731) ([olim7t](https://github.com/olim7t))

**Closed issues:**

- REST API select \* from table [\#727](https://github.com/stargate/stargate/issues/727)

**Merged pull requests:**

- Bumping version for next release [\#724](https://github.com/stargate/stargate/pull/724) ([github-actions[bot]](https://github.com/apps/github-actions))
- General cleanup of  RestApiv2Test [\#723](https://github.com/stargate/stargate/pull/723) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix handling of LIMIT in query builder \(fixes \#716\) [\#719](https://github.com/stargate/stargate/pull/719) ([olim7t](https://github.com/olim7t))

## [v1.0.12](https://github.com/stargate/stargate/tree/v1.0.12) (2021-02-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.11...v1.0.12)

**Closed issues:**

- Integration tests fail to run on mac [\#702](https://github.com/stargate/stargate/issues/702)

**Merged pull requests:**

- Preserve environment of parent process in ProcessRunner [\#721](https://github.com/stargate/stargate/pull/721) ([olim7t](https://github.com/olim7t))
- Unlogged batches property [\#717](https://github.com/stargate/stargate/pull/717) ([EricBorczuk](https://github.com/EricBorczuk))
- Fix boundStatementsUnsetTest [\#715](https://github.com/stargate/stargate/pull/715) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#714](https://github.com/stargate/stargate/pull/714) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.11](https://github.com/stargate/stargate/tree/v1.0.11) (2021-02-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.10...v1.0.11)

**Implemented enhancements:**

- Replace String padding code by library call [\#536](https://github.com/stargate/stargate/issues/536)

**Merged pull requests:**

- Make system\_keyspaces\_filtering configurable [\#712](https://github.com/stargate/stargate/pull/712) ([dimas-b](https://github.com/dimas-b))
- Update codacy coverage version to not use bintray [\#708](https://github.com/stargate/stargate/pull/708) ([lolgab](https://github.com/lolgab))
- Bumping version for next release [\#707](https://github.com/stargate/stargate/pull/707) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#536 - Replace String padding by library call [\#699](https://github.com/stargate/stargate/pull/699) ([eribeiro](https://github.com/eribeiro))
- Do what filters we can in cassandra [\#681](https://github.com/stargate/stargate/pull/681) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.10](https://github.com/stargate/stargate/tree/v1.0.10) (2021-02-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.9...v1.0.10)

**Closed issues:**

- Provide counter cql column update/write feature [\#688](https://github.com/stargate/stargate/issues/688)

**Merged pull requests:**

- move --bind-to-listen-address to starctl cmd [\#705](https://github.com/stargate/stargate/pull/705) ([jtgrabowski](https://github.com/jtgrabowski))
- Add ability to update counters in restapi [\#703](https://github.com/stargate/stargate/pull/703) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Use superuser Query/Client state for external users. [\#701](https://github.com/stargate/stargate/pull/701) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#693](https://github.com/stargate/stargate/pull/693) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.9](https://github.com/stargate/stargate/tree/v1.0.9) (2021-02-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.8...v1.0.9)

**Fixed bugs:**

- DEVELOPER\_MODE starctl option is enabled when value other than 'true' is used [\#673](https://github.com/stargate/stargate/issues/673)

**Closed issues:**

- Minimal set of projects needed for REST API [\#691](https://github.com/stargate/stargate/issues/691)
- PersistenceDataStoreFactory does not call login for external users [\#683](https://github.com/stargate/stargate/issues/683)
- ERROR: Bundle io.stargate.db.cassandra EventDispatcher: Error during dispatch. [\#678](https://github.com/stargate/stargate/issues/678)
- Increase default internal page size for searching the docs API [\#656](https://github.com/stargate/stargate/issues/656)
- Batch write endpoint for the documents API [\#655](https://github.com/stargate/stargate/issues/655)
- Consider simpler alternative to Apollo library for GraphQL ITs [\#270](https://github.com/stargate/stargate/issues/270)

**Merged pull requests:**

- Add Netty memory metrics to CQL [\#692](https://github.com/stargate/stargate/pull/692) ([mpenick](https://github.com/mpenick))
- Always login users into Persistence Connections [\#685](https://github.com/stargate/stargate/pull/685) ([dimas-b](https://github.com/dimas-b))
- STAR-157 parent pom build fix [\#676](https://github.com/stargate/stargate/pull/676) ([jtgrabowski](https://github.com/jtgrabowski))
- Check that env var DEVELOPER\_MODE is set and 'true' before enabling developer mode [\#674](https://github.com/stargate/stargate/pull/674) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Make update\_changelog.sh runnable on Linux [\#671](https://github.com/stargate/stargate/pull/671) ([dimas-b](https://github.com/dimas-b))
- Increase page size default to 1000, turn it into a configurable value [\#670](https://github.com/stargate/stargate/pull/670) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version for next release [\#669](https://github.com/stargate/stargate/pull/669) ([github-actions[bot]](https://github.com/apps/github-actions))
- Expose metrics mbeans [\#665](https://github.com/stargate/stargate/pull/665) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.8](https://github.com/stargate/stargate/tree/v1.0.8) (2021-02-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.7...v1.0.8)

**Closed issues:**

- Sporadic \(but frequent\) Codacy Coverage Reporter error in CI: SSL\_ERROR\_SYSCALL in connection to api.bintray.com [\#652](https://github.com/stargate/stargate/issues/652)
- Sporadic test failure in AuthorizationCommandInterceptorTest: unable to capture logs in time [\#651](https://github.com/stargate/stargate/issues/651)
- \[FEAT\] Website: Redirect to Code/Stargazers/Fork menu [\#646](https://github.com/stargate/stargate/issues/646)
- Include schema agreement into the liveness check [\#636](https://github.com/stargate/stargate/issues/636)
- Use Cassandra 4.0-beta4 \(requires changes in schema handling code\) [\#578](https://github.com/stargate/stargate/issues/578)
- Fix CassandraMetricsRegistry to be compatible with 4.0-beta3 [\#577](https://github.com/stargate/stargate/issues/577)
- Add README instructions for running test in local dev. env. [\#195](https://github.com/stargate/stargate/issues/195)

**Merged pull requests:**

- Support stargate.broadcast\_address for C\* 3.11 and DSE [\#666](https://github.com/stargate/stargate/pull/666) ([dimas-b](https://github.com/dimas-b))
- Use Cassandra 4.0-beta4 [\#662](https://github.com/stargate/stargate/pull/662) ([dimas-b](https://github.com/dimas-b))
- Make LogCollector wait for the expected number of messages. [\#657](https://github.com/stargate/stargate/pull/657) ([dimas-b](https://github.com/dimas-b))
- Determine CODACY\_REPORTER\_VERSION once per test execution [\#654](https://github.com/stargate/stargate/pull/654) ([dimas-b](https://github.com/dimas-b))
- Use Cassandra 4.0-beta3 [\#653](https://github.com/stargate/stargate/pull/653) ([dimas-b](https://github.com/dimas-b))
- Test whether schema argreement is achievable during the liveness check. [\#648](https://github.com/stargate/stargate/pull/648) ([dimas-b](https://github.com/dimas-b))
- Convenience script for updating the changelog [\#644](https://github.com/stargate/stargate/pull/644) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Make saveToken public to allow for use with other auth mechanisms [\#643](https://github.com/stargate/stargate/pull/643) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#641](https://github.com/stargate/stargate/pull/641) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use custom runners for storage node processes [\#640](https://github.com/stargate/stargate/pull/640) ([dimas-b](https://github.com/dimas-b))
- Update readme for consistency [\#638](https://github.com/stargate/stargate/pull/638) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v1.0.7](https://github.com/stargate/stargate/tree/v1.0.7) (2021-02-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.6...v1.0.7)

**Closed issues:**

- Unable to query data from a valid json inserted using Documents API with stargate 1.0.5 and DSE 6.8.9 [\#632](https://github.com/stargate/stargate/issues/632)
- GraphQL: fix aliased sub-selections in queries [\#627](https://github.com/stargate/stargate/issues/627)
- Readiness check should wait for all bundles to start [\#620](https://github.com/stargate/stargate/issues/620)
- Support multiple `where` clauses over multiple `fields` \(still using only AND\) [\#572](https://github.com/stargate/stargate/issues/572)

**Merged pull requests:**

- Make toString of ClientInfo nullsafe [\#635](https://github.com/stargate/stargate/pull/635) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Support stargate.unsafe.cassandra\_config\_path in DSE Persistence [\#633](https://github.com/stargate/stargate/pull/633) ([dimas-b](https://github.com/dimas-b))
- Initial rate limiting capability proposal for Stargate [\#629](https://github.com/stargate/stargate/pull/629) ([pcmanus](https://github.com/pcmanus))
- Upgrade graphql-java to 16.1 [\#628](https://github.com/stargate/stargate/pull/628) ([olim7t](https://github.com/olim7t))
- Include health checks in the readiness check [\#626](https://github.com/stargate/stargate/pull/626) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#625](https://github.com/stargate/stargate/pull/625) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add ability to search across collections with multiple `where` clauses [\#614](https://github.com/stargate/stargate/pull/614) ([EricBorczuk](https://github.com/EricBorczuk))
- Add a startctl option for cassandra.yaml [\#602](https://github.com/stargate/stargate/pull/602) ([marksurnin](https://github.com/marksurnin))
- GraphQL: refactor codebase in anticipation of schema-first API [\#592](https://github.com/stargate/stargate/pull/592) ([olim7t](https://github.com/olim7t))
- API for authorization processors [\#554](https://github.com/stargate/stargate/pull/554) ([dimas-b](https://github.com/dimas-b))

## [v1.0.6](https://github.com/stargate/stargate/tree/v1.0.6) (2021-01-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.5...v1.0.6)

**Closed issues:**

- "\_\_conversionWarnings" violates graphQL introspection naming schema [\#615](https://github.com/stargate/stargate/issues/615)

**Merged pull requests:**

- Add decorate keyspace method for graphql cache [\#623](https://github.com/stargate/stargate/pull/623) ([tjake](https://github.com/tjake))
- Increase DSE requests timeouts on the Stargate side in tests [\#622](https://github.com/stargate/stargate/pull/622) ([dimas-b](https://github.com/dimas-b))
- Update OKHttp version [\#621](https://github.com/stargate/stargate/pull/621) ([mpenick](https://github.com/mpenick))
- GraphQL: Remove "\_\_" prefix from "conversionWarnings" query name [\#616](https://github.com/stargate/stargate/pull/616) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#612](https://github.com/stargate/stargate/pull/612) ([github-actions[bot]](https://github.com/apps/github-actions))
- Do not pull schema from non-token ring members [\#611](https://github.com/stargate/stargate/pull/611) ([mpenick](https://github.com/mpenick))
- Update DSE persistence to 6.8.9 [\#607](https://github.com/stargate/stargate/pull/607) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Docs API changes to improve performance [\#534](https://github.com/stargate/stargate/pull/534) ([EricBorczuk](https://github.com/EricBorczuk))

## [v1.0.5](https://github.com/stargate/stargate/tree/v1.0.5) (2021-01-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.4...v1.0.5)

**Merged pull requests:**

- Do not pull schema from non-token ring members [\#611](https://github.com/stargate/stargate/pull/611) ([mpenick](https://github.com/mpenick))
- Fix key check for customPayload in StargateQueryHandler [\#610](https://github.com/stargate/stargate/pull/610) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Pass custom properties to persistence backend [\#609](https://github.com/stargate/stargate/pull/609) ([tjake](https://github.com/tjake))
- Bumping version for next release [\#608](https://github.com/stargate/stargate/pull/608) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.4](https://github.com/stargate/stargate/tree/v1.0.4) (2021-01-20)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.3...v1.0.4)

**Fixed bugs:**

- Persistence bundle startup failure does not cause Stargate process failure [\#591](https://github.com/stargate/stargate/issues/591)

**Merged pull requests:**

- Adds cql event filters so persistence can decide if events should be sent to a given connection [\#605](https://github.com/stargate/stargate/pull/605) ([tjake](https://github.com/tjake))
- Encapsulate auth data extras [\#604](https://github.com/stargate/stargate/pull/604) ([dimas-b](https://github.com/dimas-b))
- Exposed root auth failure messages [\#601](https://github.com/stargate/stargate/pull/601) ([dimas-b](https://github.com/dimas-b))
- Run DSE tests first in CI [\#600](https://github.com/stargate/stargate/pull/600) ([dimas-b](https://github.com/dimas-b))
- Make ClientInfo buffers reusable [\#599](https://github.com/stargate/stargate/pull/599) ([dimas-b](https://github.com/dimas-b))
- Change default TPC core count for Stargate [\#598](https://github.com/stargate/stargate/pull/598) ([tjake](https://github.com/tjake))
- Bumping version for next release [\#596](https://github.com/stargate/stargate/pull/596) ([github-actions[bot]](https://github.com/apps/github-actions))
- Additional metrics for stargate [\#589](https://github.com/stargate/stargate/pull/589) ([tomekl007](https://github.com/tomekl007))

## [v1.0.3](https://github.com/stargate/stargate/tree/v1.0.3) (2021-01-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.2...v1.0.3)

**Fixed bugs:**

- Frequent timeouts in BatchStatementTest with DSE in CI [\#588](https://github.com/stargate/stargate/issues/588)
- Swagger should use proxy path in examples [\#563](https://github.com/stargate/stargate/issues/563)

**Merged pull requests:**

- Terminate Stargate java process in case of service start errors. [\#594](https://github.com/stargate/stargate/pull/594) ([dimas-b](https://github.com/dimas-b))
- Add conversion warnings doc section to graphqlapi [\#593](https://github.com/stargate/stargate/pull/593) ([mpenick](https://github.com/mpenick))
- Increase heartbeat timeout and interval in test [\#590](https://github.com/stargate/stargate/pull/590) ([dimas-b](https://github.com/dimas-b))
- Use proxy path in Swagger examples \(fixes \#563\) [\#585](https://github.com/stargate/stargate/pull/585) ([olim7t](https://github.com/olim7t))
- Improve NodeTool exception handling in Starter [\#584](https://github.com/stargate/stargate/pull/584) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#582](https://github.com/stargate/stargate/pull/582) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#545 - NPE is thrown if TableOptions is null [\#559](https://github.com/stargate/stargate/pull/559) ([eribeiro](https://github.com/eribeiro))

## [v1.0.2](https://github.com/stargate/stargate/tree/v1.0.2) (2021-01-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.1...v1.0.2)

**Fixed bugs:**

- NPE is thrown if tableOptions are not specified in v1 create table [\#545](https://github.com/stargate/stargate/issues/545)

**Closed issues:**

- Use Cassandra 4.0-beta2 for compilation [\#574](https://github.com/stargate/stargate/issues/574)
- Use java.awt.headless=true java system property [\#568](https://github.com/stargate/stargate/issues/568)
- Expose ability to manage indexes in GraphQL [\#543](https://github.com/stargate/stargate/issues/543)
- Refactor DataStore initialization to be not static in all places [\#539](https://github.com/stargate/stargate/issues/539)
- DEV\_GUIDE.md should mention jdk 8 as a requirement  [\#453](https://github.com/stargate/stargate/issues/453)
- Docs API not listed in build [\#420](https://github.com/stargate/stargate/issues/420)
- Document API unit tests are not run by CI because they still use JUnit 4 features [\#283](https://github.com/stargate/stargate/issues/283)
- External Auth/Adding Claims [\#122](https://github.com/stargate/stargate/issues/122)

**Merged pull requests:**

- Use Cassandra 4.0-beta2 for compilation [\#576](https://github.com/stargate/stargate/pull/576) ([dimas-b](https://github.com/dimas-b))
- Add Starter options to allow running NodeTool [\#573](https://github.com/stargate/stargate/pull/573) ([dimas-b](https://github.com/dimas-b))
- Set java.awt.headless=true in starctl [\#569](https://github.com/stargate/stargate/pull/569) ([dimas-b](https://github.com/dimas-b))
- Fixes \#453 - Mention jdk8 requirement [\#566](https://github.com/stargate/stargate/pull/566) ([eribeiro](https://github.com/eribeiro))
- Adds the source IP and port to the proxy-protocol detection and Clien… [\#564](https://github.com/stargate/stargate/pull/564) ([tjake](https://github.com/tjake))
- Add index mutation for GraphQL [\#562](https://github.com/stargate/stargate/pull/562) ([eribeiro](https://github.com/eribeiro))
- Make the BaseActivator\#stopService\(\) method not required [\#560](https://github.com/stargate/stargate/pull/560) ([tomekl007](https://github.com/tomekl007))
- Build timeout to 4hr [\#557](https://github.com/stargate/stargate/pull/557) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Make builds serial for now [\#556](https://github.com/stargate/stargate/pull/556) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add logback to release [\#553](https://github.com/stargate/stargate/pull/553) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#548](https://github.com/stargate/stargate/pull/548) ([github-actions[bot]](https://github.com/apps/github-actions))
- Data store as a service [\#540](https://github.com/stargate/stargate/pull/540) ([tomekl007](https://github.com/tomekl007))
- Refactor authn/z to pass around object rather than plain token string [\#526](https://github.com/stargate/stargate/pull/526) ([dougwettlaufer](https://github.com/dougwettlaufer))
- REST: Revisit error messages when parsing values [\#517](https://github.com/stargate/stargate/pull/517) ([olim7t](https://github.com/olim7t))
- GraphQL: Cover JWT authentication in integration tests [\#507](https://github.com/stargate/stargate/pull/507) ([olim7t](https://github.com/olim7t))
- Add ErrorProne to the build [\#424](https://github.com/stargate/stargate/pull/424) ([olim7t](https://github.com/olim7t))

## [v1.0.1](https://github.com/stargate/stargate/tree/v1.0.1) (2020-12-17)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.0...v1.0.1)

**Fixed bugs:**

- Put `AuthResponse` validation on a separate thread pool [\#529](https://github.com/stargate/stargate/issues/529)
- DocumentDBTest does not run in CI [\#527](https://github.com/stargate/stargate/issues/527)
- Creating a table using REAT API with a column definition of map containing space in between causes 404 [\#522](https://github.com/stargate/stargate/issues/522)

**Closed issues:**

- BaseActivator does not work when no dependent services are needed [\#541](https://github.com/stargate/stargate/issues/541)
- Question: what should I use as the SEED param when running in Kubernetes? [\#535](https://github.com/stargate/stargate/issues/535)

**Merged pull requests:**

- Modify validation of listen address to support IPv4 and IPv6 [\#546](https://github.com/stargate/stargate/pull/546) ([dougwettlaufer](https://github.com/dougwettlaufer))
- BaseActivator does not work when no dependent services are needed [\#542](https://github.com/stargate/stargate/pull/542) ([tomekl007](https://github.com/tomekl007))
- Support service access permissions [\#538](https://github.com/stargate/stargate/pull/538) ([tomekl007](https://github.com/tomekl007))
- Re-include Docs API unit tests \(fixes \#527\) [\#531](https://github.com/stargate/stargate/pull/531) ([dimas-b](https://github.com/dimas-b))
- Fix: Move CQL AuthResponse request handling to request threads [\#530](https://github.com/stargate/stargate/pull/530) ([mpenick](https://github.com/mpenick))
- Declare support for SAI in Persistence [\#528](https://github.com/stargate/stargate/pull/528) ([dimas-b](https://github.com/dimas-b))
- Fix \#522 - Allow space between CQL base types and parameters [\#525](https://github.com/stargate/stargate/pull/525) ([eribeiro](https://github.com/eribeiro))
- Add changelog [\#523](https://github.com/stargate/stargate/pull/523) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#518](https://github.com/stargate/stargate/pull/518) ([github-actions[bot]](https://github.com/apps/github-actions))
- Make RestApiJWTAuthTest runnable with Java 11 [\#516](https://github.com/stargate/stargate/pull/516) ([dimas-b](https://github.com/dimas-b))

## [v1.0.0](https://github.com/stargate/stargate/tree/v1.0.0) (2020-12-09)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.29...v1.0.0)

**Merged pull requests:**

- Set version to 1.0.0 [\#515](https://github.com/stargate/stargate/pull/515) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#513](https://github.com/stargate/stargate/pull/513) ([github-actions[bot]](https://github.com/apps/github-actions))
- DKG: Updating Stargate Architecture [\#508](https://github.com/stargate/stargate/pull/508) ([denisekgosnell](https://github.com/denisekgosnell))
- Add support for AuthenticationStatement and AuthorizationStatement in new auth models [\#502](https://github.com/stargate/stargate/pull/502) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.29](https://github.com/stargate/stargate/tree/v0.0.29) (2020-12-08)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.28...v0.0.29)

**Merged pull requests:**

- Fix search for tinyint bools [\#510](https://github.com/stargate/stargate/pull/510) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version for next release [\#506](https://github.com/stargate/stargate/pull/506) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add new authz to docsapi [\#498](https://github.com/stargate/stargate/pull/498) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.28](https://github.com/stargate/stargate/tree/v0.0.28) (2020-12-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.27...v0.0.28)

**Fixed bugs:**

- GraphQl should prevent schema modification on tables with only SELECT role. [\#449](https://github.com/stargate/stargate/issues/449)

**Closed issues:**

- Extend BaseActivator with lazy init support and migrate all `CassandraActivators` [\#443](https://github.com/stargate/stargate/issues/443)
- Allow submodules to expose their own HealthChecks and register them in the health-checker [\#427](https://github.com/stargate/stargate/issues/427)
- Support tuples and UDTs in RestAPI [\#72](https://github.com/stargate/stargate/issues/72)

**Merged pull requests:**

- Also support bearer token in swagger and graphql [\#503](https://github.com/stargate/stargate/pull/503) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Use a tinyint for backends where secondary indexes are not supported [\#501](https://github.com/stargate/stargate/pull/501) ([EricBorczuk](https://github.com/EricBorczuk))
- Add cpu metrics [\#493](https://github.com/stargate/stargate/pull/493) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL: Adapt DropKeyspaceFetcher to new query object API [\#456](https://github.com/stargate/stargate/pull/456) ([olim7t](https://github.com/olim7t))
- REST: Allow non-string JSON types for incoming column data \(fixes \#49\) [\#455](https://github.com/stargate/stargate/pull/455) ([olim7t](https://github.com/olim7t))
- Remove outdated CI badge [\#454](https://github.com/stargate/stargate/pull/454) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add GRANT PERMISSIONS statements to CQL [\#452](https://github.com/stargate/stargate/pull/452) ([polandll](https://github.com/polandll))
- Fix cassandra authz \(fixes \#449\) [\#451](https://github.com/stargate/stargate/pull/451) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Kafka CDC Producer Health Checks [\#448](https://github.com/stargate/stargate/pull/448) ([tomekl007](https://github.com/tomekl007))
- Bumping version for next release [\#447](https://github.com/stargate/stargate/pull/447) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use DSE 6.8.7 [\#446](https://github.com/stargate/stargate/pull/446) ([dimas-b](https://github.com/dimas-b))
- Extend BaseActivator with lazy init support and migrate all `CassandraActivators` [\#445](https://github.com/stargate/stargate/pull/445) ([tomekl007](https://github.com/tomekl007))
- Update licenses-report.txt [\#442](https://github.com/stargate/stargate/pull/442) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Update swaggerui to work behind a proxy [\#441](https://github.com/stargate/stargate/pull/441) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Replace query strings by query objects in DataStore [\#413](https://github.com/stargate/stargate/pull/413) ([pcmanus](https://github.com/pcmanus))

## [v0.0.27](https://github.com/stargate/stargate/tree/v0.0.27) (2020-12-01)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.26...v0.0.27)

**Closed issues:**

- Need dropKeyspace in graphQL [\#410](https://github.com/stargate/stargate/issues/410)
- Migrate all existing activators to use the `BaseActivator` [\#381](https://github.com/stargate/stargate/issues/381)

**Merged pull requests:**

- Fix de-registration of services in the BaseActivator [\#444](https://github.com/stargate/stargate/pull/444) ([tomekl007](https://github.com/tomekl007))
- GraphQL: Add authorization check in dropKeyspace [\#440](https://github.com/stargate/stargate/pull/440) ([olim7t](https://github.com/olim7t))
- Report full root cause exception in PersistenceTest.testUseTypesInWhere [\#438](https://github.com/stargate/stargate/pull/438) ([dimas-b](https://github.com/dimas-b))
- Updating architecture image [\#437](https://github.com/stargate/stargate/pull/437) ([drewwitmer](https://github.com/drewwitmer))
- Migrate activators to `BaseActivator` [\#436](https://github.com/stargate/stargate/pull/436) ([tomekl007](https://github.com/tomekl007))
- Bumping version for next release [\#435](https://github.com/stargate/stargate/pull/435) ([github-actions[bot]](https://github.com/apps/github-actions))
- Publish health checks from each bundle [\#431](https://github.com/stargate/stargate/pull/431) ([olim7t](https://github.com/olim7t))
- Use single-port Dropwizard servers for all web resources [\#429](https://github.com/stargate/stargate/pull/429) ([olim7t](https://github.com/olim7t))
- GraphQL: Add dropKeyspace mutation \(fixes \#410\) [\#416](https://github.com/stargate/stargate/pull/416) ([olim7t](https://github.com/olim7t))
- Add new auth-jwt-service [\#339](https://github.com/stargate/stargate/pull/339) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.26](https://github.com/stargate/stargate/tree/v0.0.26) (2020-11-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.25...v0.0.26)

**Merged pull requests:**

- Increase query trace fetching attempts in tests [\#433](https://github.com/stargate/stargate/pull/433) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#432](https://github.com/stargate/stargate/pull/432) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v0.0.25](https://github.com/stargate/stargate/tree/v0.0.25) (2020-11-24)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.24...v0.0.25)

**Closed issues:**

- CQL: Deterministically generate `tokens` from the node's address [\#373](https://github.com/stargate/stargate/issues/373)

**Merged pull requests:**

- Allow test backends to declare counter support [\#430](https://github.com/stargate/stargate/pull/430) ([dimas-b](https://github.com/dimas-b))
- Make GraphqlTest runnable with Java 11 [\#426](https://github.com/stargate/stargate/pull/426) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#425](https://github.com/stargate/stargate/pull/425) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v0.0.24](https://github.com/stargate/stargate/tree/v0.0.24) (2020-11-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.23...v0.0.24)

**Closed issues:**

- GraphQL: Switch the web stack to DropWizard [\#400](https://github.com/stargate/stargate/issues/400)
- GraphQL: extract 'FilterOperator' enum [\#307](https://github.com/stargate/stargate/issues/307)
- GraphQL: add UDT DDL operations [\#285](https://github.com/stargate/stargate/issues/285)
- Upgrade graphql-java-servlet dependency once it supports async [\#131](https://github.com/stargate/stargate/issues/131)

**Merged pull requests:**

- Update CODEOWNERS [\#423](https://github.com/stargate/stargate/pull/423) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Change `action/setup-java` to `@v1` for publish step [\#417](https://github.com/stargate/stargate/pull/417) ([mpenick](https://github.com/mpenick))
- Fix release action by upgrading `action/setup-java` [\#415](https://github.com/stargate/stargate/pull/415) ([mpenick](https://github.com/mpenick))
- GraphQL: use full URI in playground tabs [\#412](https://github.com/stargate/stargate/pull/412) ([olim7t](https://github.com/olim7t))
- CQL: Auto-detect proxy protocol [\#411](https://github.com/stargate/stargate/pull/411) ([mpenick](https://github.com/mpenick))
- Migrate GraphQL web stack to DropWizard [\#405](https://github.com/stargate/stargate/pull/405) ([olim7t](https://github.com/olim7t))
- Add @ManagedAsync to document API endpoints [\#404](https://github.com/stargate/stargate/pull/404) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version for next release [\#402](https://github.com/stargate/stargate/pull/402) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL: Add DDL operations for UDTs \(fixes \#285\) [\#397](https://github.com/stargate/stargate/pull/397) ([olim7t](https://github.com/olim7t))
- CQL: Deterministically generate `tokens` from the node's address [\#392](https://github.com/stargate/stargate/pull/392) ([mpenick](https://github.com/mpenick))

## [v0.0.23](https://github.com/stargate/stargate/tree/v0.0.23) (2020-11-13)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.22...v0.0.23)

**Implemented enhancements:**

- GraphQL: Add support for tuple type [\#233](https://github.com/stargate/stargate/issues/233)

**Closed issues:**

- GraphQL: Use local quorum as default consistency level [\#388](https://github.com/stargate/stargate/issues/388)
- Stargate doesn't report proper options in the SupportedMessage [\#376](https://github.com/stargate/stargate/issues/376)
- GraphQL schema processing error in CI \(shouldSupportSingleMutationWithAtomicDirective\) [\#365](https://github.com/stargate/stargate/issues/365)
- Refactor Activators common code and improve its testability  [\#364](https://github.com/stargate/stargate/issues/364)
- GraphQL: Add source table info in the description of types, queries and mutations generated [\#342](https://github.com/stargate/stargate/issues/342)
- GraphQL: don't allow integer literals for Decimal type [\#337](https://github.com/stargate/stargate/issues/337)
- Add proxy protocol integration tests [\#322](https://github.com/stargate/stargate/issues/322)
- GraphQL gets a schema notification for non-existent keyspace [\#247](https://github.com/stargate/stargate/issues/247)
- GraphQL mutations do not respect camelCase definitions [\#176](https://github.com/stargate/stargate/issues/176)
- Cassandra to Graphql Naming Resolution [\#118](https://github.com/stargate/stargate/issues/118)

**Merged pull requests:**

- Allow customizing integration test resource lifecycle [\#398](https://github.com/stargate/stargate/pull/398) ([dimas-b](https://github.com/dimas-b))
- When supplying a where clause to the "full document" search, the path should be matched exactly [\#395](https://github.com/stargate/stargate/pull/395) ([EricBorczuk](https://github.com/EricBorczuk))
- GraphQL defaults [\#391](https://github.com/stargate/stargate/pull/391) ([jorgebay](https://github.com/jorgebay))
- Add basic support for URL-encoded forms in Documents API [\#390](https://github.com/stargate/stargate/pull/390) ([EricBorczuk](https://github.com/EricBorczuk))
- GraphQL: Use enum for filter operators [\#389](https://github.com/stargate/stargate/pull/389) ([jorgebay](https://github.com/jorgebay))
-  Change the `getWithOverrides\(\)` to return String type. Add the version that takes the user-provided mapper. [\#387](https://github.com/stargate/stargate/pull/387) ([tomekl007](https://github.com/tomekl007))
- CQL: Fix supported options [\#379](https://github.com/stargate/stargate/pull/379) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#378](https://github.com/stargate/stargate/pull/378) ([github-actions[bot]](https://github.com/apps/github-actions))
- Include description for types, queries and mutations generated [\#375](https://github.com/stargate/stargate/pull/375) ([jorgebay](https://github.com/jorgebay))
- make config-store identifier key name unique [\#374](https://github.com/stargate/stargate/pull/374) ([tomekl007](https://github.com/tomekl007))
- Refactor Activators common code and improve its testability [\#370](https://github.com/stargate/stargate/pull/370) ([tomekl007](https://github.com/tomekl007))
- CQL: Proxy protocol integration tests [\#361](https://github.com/stargate/stargate/pull/361) ([mpenick](https://github.com/mpenick))
- CQL: Add integration tests for `system.local` and `system.peers` [\#351](https://github.com/stargate/stargate/pull/351) ([mpenick](https://github.com/mpenick))
- Revisit GraphQL naming conventions \(fixes \#118\) [\#216](https://github.com/stargate/stargate/pull/216) ([olim7t](https://github.com/olim7t))

## [v0.0.22](https://github.com/stargate/stargate/tree/v0.0.22) (2020-11-04)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.21...v0.0.22)

**Closed issues:**

- Add methods for extracting setting of an expected type from the config-map using config-store API. [\#363](https://github.com/stargate/stargate/issues/363)
- Support auto generating timeuuid fieldsSupport auto generating timeuuid fields [\#132](https://github.com/stargate/stargate/issues/132)

**Merged pull requests:**

- Set graphql to only bind to listen address when stargate.bind\_to\_listen\_address is true [\#377](https://github.com/stargate/stargate/pull/377) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#372](https://github.com/stargate/stargate/pull/372) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL: Support now\(\) and uuid\(\) functions [\#369](https://github.com/stargate/stargate/pull/369) ([jorgebay](https://github.com/jorgebay))
- Add methods for extracting setting of an expected type from the config-map using config-store API [\#367](https://github.com/stargate/stargate/pull/367) ([tomekl007](https://github.com/tomekl007))

## [v0.0.21](https://github.com/stargate/stargate/tree/v0.0.21) (2020-11-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.20...v0.0.21)

**Implemented enhancements:**

- GraphQL: Use driver codecs for scalar parsing [\#326](https://github.com/stargate/stargate/issues/326)

**Closed issues:**

- Cache file read by config-store-yaml to improve performance [\#340](https://github.com/stargate/stargate/issues/340)
- Improve error handling when building GraphQL schema [\#256](https://github.com/stargate/stargate/issues/256)

**Merged pull requests:**

- GraphQL: Surface conversion errors to the client \(fixes \#256\) [\#354](https://github.com/stargate/stargate/pull/354) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#352](https://github.com/stargate/stargate/pull/352) ([github-actions[bot]](https://github.com/apps/github-actions))
- Config-store overrides at a higher level. [\#350](https://github.com/stargate/stargate/pull/350) ([tomekl007](https://github.com/tomekl007))
- GraphQL: Support more formats for custom scalars \(fixes \#326\) [\#348](https://github.com/stargate/stargate/pull/348) ([olim7t](https://github.com/olim7t))
- Bind to 0.0.0.0 for cql unless flag set \(matching other services\) [\#347](https://github.com/stargate/stargate/pull/347) ([tjake](https://github.com/tjake))
- Cache file read by config-store-yaml to improve performance [\#343](https://github.com/stargate/stargate/pull/343) ([tomekl007](https://github.com/tomekl007))
- REST: Support collection operators [\#313](https://github.com/stargate/stargate/pull/313) ([olim7t](https://github.com/olim7t))

## [v0.0.20](https://github.com/stargate/stargate/tree/v0.0.20) (2020-10-28)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.19...v0.0.20)

**Fixed bugs:**

- GraphQL Scalars: Blob encoding to string is broken [\#304](https://github.com/stargate/stargate/issues/304)

**Closed issues:**

- Config-Store API supports overriding settings [\#302](https://github.com/stargate/stargate/issues/302)
- Implement Config-Store API based on the K8s config-map API [\#300](https://github.com/stargate/stargate/issues/300)
- Use graphql-java builtin scalars for CQL scalars [\#272](https://github.com/stargate/stargate/issues/272)
- GraphQL: fix handling of DATE type [\#268](https://github.com/stargate/stargate/issues/268)
- GraphQL: Create integration tests with every supported scalar type [\#261](https://github.com/stargate/stargate/issues/261)
- GraphQL: support collections in primary keys [\#259](https://github.com/stargate/stargate/issues/259)
- Fix flaky test MultipleStargateInstancesTest.shouldDistributeTrafficUniformly [\#232](https://github.com/stargate/stargate/issues/232)
- Support Duration GraphQL scalar types [\#206](https://github.com/stargate/stargate/issues/206)
- Publish tests so new persistence backends can be tested [\#197](https://github.com/stargate/stargate/issues/197)

**Merged pull requests:**

- Fix path for cache exclusion in cloudbuild.yaml [\#346](https://github.com/stargate/stargate/pull/346) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add consistency level to delete [\#345](https://github.com/stargate/stargate/pull/345) ([EricBorczuk](https://github.com/EricBorczuk))
- increment version of stargate for config-store [\#341](https://github.com/stargate/stargate/pull/341) ([tomekl007](https://github.com/tomekl007))
- Increase default CQL schema agreement timeout [\#338](https://github.com/stargate/stargate/pull/338) ([dimas-b](https://github.com/dimas-b))
- Further refactoring of starter so it can be subclassed [\#336](https://github.com/stargate/stargate/pull/336) ([tjake](https://github.com/tjake))
- Create a Junit 5 extension to manage driver sessions [\#335](https://github.com/stargate/stargate/pull/335) ([olim7t](https://github.com/olim7t))
- Update README.md [\#333](https://github.com/stargate/stargate/pull/333) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Run DDL once in GraphqlTest [\#330](https://github.com/stargate/stargate/pull/330) ([dimas-b](https://github.com/dimas-b))
- Make Integration Tests runnable in Console Launcher \(\#197\) [\#327](https://github.com/stargate/stargate/pull/327) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#324](https://github.com/stargate/stargate/pull/324) ([github-actions[bot]](https://github.com/apps/github-actions))
- Automatically release to nexus on deploy [\#323](https://github.com/stargate/stargate/pull/323) ([dougwettlaufer](https://github.com/dougwettlaufer))
- GraphQL scalars: fixes and tests [\#317](https://github.com/stargate/stargate/pull/317) ([jorgebay](https://github.com/jorgebay))
- GraphQL: various fixes [\#316](https://github.com/stargate/stargate/pull/316) ([olim7t](https://github.com/olim7t))
- Implement Config-Store API based on the K8s config-map API [\#305](https://github.com/stargate/stargate/pull/305) ([tomekl007](https://github.com/tomekl007))
- Add UDT support to GraphQL \(fixes \#126\) [\#271](https://github.com/stargate/stargate/pull/271) ([olim7t](https://github.com/olim7t))
- Add one Column Resource Unit Tests [\#265](https://github.com/stargate/stargate/pull/265) ([FRosner](https://github.com/FRosner))
- Add JUnit 5 extension for managing Stargate nodes [\#254](https://github.com/stargate/stargate/pull/254) ([dimas-b](https://github.com/dimas-b))
- Wait until the driver sees all nodes in tests \(fixes \#232\) [\#245](https://github.com/stargate/stargate/pull/245) ([olim7t](https://github.com/olim7t))

## [v0.0.19](https://github.com/stargate/stargate/tree/v0.0.19) (2020-10-23)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.18...v0.0.19)

**Closed issues:**

- DSE persistence does not support SAI indexes [\#298](https://github.com/stargate/stargate/issues/298)
- Documents: endpoints for listing collections, updating the name of a collection, deleting a collection [\#266](https://github.com/stargate/stargate/issues/266)
- Use the RPC port for TOPOLOGY\_CHANGE events [\#250](https://github.com/stargate/stargate/issues/250)
- Add tests for auth-api [\#83](https://github.com/stargate/stargate/issues/83)

**Merged pull requests:**

- Use ClientStateWithPublicAddress to support proxy on internal actions in DSE [\#321](https://github.com/stargate/stargate/pull/321) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Dse class visibilty [\#320](https://github.com/stargate/stargate/pull/320) ([tjake](https://github.com/tjake))
- Increase timeouts due to slow CI [\#315](https://github.com/stargate/stargate/pull/315) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#311](https://github.com/stargate/stargate/pull/311) ([github-actions[bot]](https://github.com/apps/github-actions))
- Upgrade DSE library dependencies to 6.8.5 \(fixes \#298\) [\#310](https://github.com/stargate/stargate/pull/310) ([olim7t](https://github.com/olim7t))
- Add collection meta endpoints; offer an upgrade path to support SAI [\#309](https://github.com/stargate/stargate/pull/309) ([EricBorczuk](https://github.com/EricBorczuk))
- Add DNS support to proxy protocol query interceptor [\#267](https://github.com/stargate/stargate/pull/267) ([mpenick](https://github.com/mpenick))

## [v0.0.18](https://github.com/stargate/stargate/tree/v0.0.18) (2020-10-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.17...v0.0.18)

**Closed issues:**

- Unable to initialize auth when using DSE backend [\#288](https://github.com/stargate/stargate/issues/288)
- Upgrade swagger to 1.5.8+ to support `@PATCH` annotation from `javax.ws.rs` [\#278](https://github.com/stargate/stargate/issues/278)
- NoClassDefFoundError in auth-table-based-service for Strings [\#144](https://github.com/stargate/stargate/issues/144)
- Add batching to mutations [\#133](https://github.com/stargate/stargate/issues/133)
- Make it possible to measure code coverage using both UT and IT [\#68](https://github.com/stargate/stargate/issues/68)

**Merged pull requests:**

- Minor cleanup of code analysis grumblings [\#299](https://github.com/stargate/stargate/pull/299) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add pipefail to test script [\#297](https://github.com/stargate/stargate/pull/297) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Relax query builder validation for custom indexes [\#296](https://github.com/stargate/stargate/pull/296) ([olim7t](https://github.com/olim7t))
- Upgrade swagger-jersey2-jaxrs [\#294](https://github.com/stargate/stargate/pull/294) ([FRosner](https://github.com/FRosner))
- Upload code coverage to codacy [\#293](https://github.com/stargate/stargate/pull/293) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#292](https://github.com/stargate/stargate/pull/292) ([github-actions[bot]](https://github.com/apps/github-actions))
- Re-login after creating table on the fly to refresh permissions [\#287](https://github.com/stargate/stargate/pull/287) ([EricBorczuk](https://github.com/EricBorczuk))
- GraphQL: add collection filters [\#286](https://github.com/stargate/stargate/pull/286) ([olim7t](https://github.com/olim7t))
- Support logged batches in GraphQL [\#284](https://github.com/stargate/stargate/pull/284) ([jorgebay](https://github.com/jorgebay))
- \#83 Auth resource tests [\#248](https://github.com/stargate/stargate/pull/248) ([FRosner](https://github.com/FRosner))

## [v0.0.17](https://github.com/stargate/stargate/tree/v0.0.17) (2020-10-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.16...v0.0.17)

**Fixed bugs:**

- GraphQL data fetchers ignore consistency level and paging options [\#208](https://github.com/stargate/stargate/issues/208)

**Closed issues:**

- Document API appears to return success with incorrect auth token [\#281](https://github.com/stargate/stargate/issues/281)
- Support UDTs as embedded types [\#126](https://github.com/stargate/stargate/issues/126)
- Don't ignore clustering key on get/delete row [\#86](https://github.com/stargate/stargate/issues/86)
- Add integration tests for auth-api [\#82](https://github.com/stargate/stargate/issues/82)

**Merged pull requests:**

- Fix initializing auth in auth-table-based-service \(fixes \#288\) [\#290](https://github.com/stargate/stargate/pull/290) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Unauthorized response [\#282](https://github.com/stargate/stargate/pull/282) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version for next release [\#280](https://github.com/stargate/stargate/pull/280) ([github-actions[bot]](https://github.com/apps/github-actions))
- GraphQL: Fix consistency and paging options [\#279](https://github.com/stargate/stargate/pull/279) ([mpenick](https://github.com/mpenick))
- Test out google code build [\#252](https://github.com/stargate/stargate/pull/252) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add swagger to auth-api [\#240](https://github.com/stargate/stargate/pull/240) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.16](https://github.com/stargate/stargate/tree/v0.0.16) (2020-10-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.15...v0.0.16)

**Fixed bugs:**

- GraphQL: bigint CQL data type incorrectly parsed as BigInteger [\#260](https://github.com/stargate/stargate/issues/260)

**Closed issues:**

- Error message when creating a namespace lists the incorrect key [\#275](https://github.com/stargate/stargate/issues/275)
- Add integration tests for nested collections in GraphQL [\#226](https://github.com/stargate/stargate/issues/226)

**Merged pull requests:**

- Swagger isn't quite working with PATCH annotations that aren't io.swagger.jaxrs [\#277](https://github.com/stargate/stargate/pull/277) ([EricBorczuk](https://github.com/EricBorczuk))
- Make `replicas` always default to 1 when missing, for SimpleStrategy [\#276](https://github.com/stargate/stargate/pull/276) ([EricBorczuk](https://github.com/EricBorczuk))
- Small fixes/additions to the Documents API [\#264](https://github.com/stargate/stargate/pull/264) ([EricBorczuk](https://github.com/EricBorczuk))
- GraphQL: test nested collections and fixes [\#263](https://github.com/stargate/stargate/pull/263) ([jorgebay](https://github.com/jorgebay))
- Bumping version for next release [\#258](https://github.com/stargate/stargate/pull/258) ([github-actions[bot]](https://github.com/apps/github-actions))
- Support prepopulated tabs and headers in playground [\#238](https://github.com/stargate/stargate/pull/238) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add parameterMacro to swagger to support prefilling the auth header [\#236](https://github.com/stargate/stargate/pull/236) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Cleanup persistence API and move DataStore on top of it [\#222](https://github.com/stargate/stargate/pull/222) ([pcmanus](https://github.com/pcmanus))
- REST v1: collection types are not reported in table v1/kesyspaces/{ks}/tables/{t} [\#220](https://github.com/stargate/stargate/pull/220) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.15](https://github.com/stargate/stargate/tree/v0.0.15) (2020-10-13)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.14...v0.0.15)

**Fixed bugs:**

- Driver not properly handling UnauthorizedException [\#210](https://github.com/stargate/stargate/issues/210)
- Columns with double data type are incorrectly mapped to GraphQLBigDecimal [\#204](https://github.com/stargate/stargate/issues/204)

**Closed issues:**

- GraphQL: implement pageSize and pageState [\#237](https://github.com/stargate/stargate/issues/237)
- GraphQL: Improve error messages [\#207](https://github.com/stargate/stargate/issues/207)
- Don't try to build persistence-dse-6.8 by default [\#202](https://github.com/stargate/stargate/issues/202)
- GraphQL schema createTable will not work on clusteringKeys without order specification [\#164](https://github.com/stargate/stargate/issues/164)
- Support Map, Set, and List types [\#141](https://github.com/stargate/stargate/issues/141)
- Support running integration tests on macOS [\#50](https://github.com/stargate/stargate/issues/50)
- Rest Api V2: not able to handle integer in post request [\#49](https://github.com/stargate/stargate/issues/49)

**Merged pull requests:**

- Add -P dse to stargate build [\#257](https://github.com/stargate/stargate/pull/257) ([EricBorczuk](https://github.com/EricBorczuk))
- Disable MultipleStargateInstancesTest [\#251](https://github.com/stargate/stargate/pull/251) ([olim7t](https://github.com/olim7t))
- add document api diagram change [\#249](https://github.com/stargate/stargate/pull/249) ([polandll](https://github.com/polandll))
- Fix failing test due to timestamp parsing \(Java 9\) [\#242](https://github.com/stargate/stargate/pull/242) ([FRosner](https://github.com/FRosner))
- Upgrade maven bundle plugin to avoid ConcurrentModificationException [\#241](https://github.com/stargate/stargate/pull/241) ([FRosner](https://github.com/FRosner))
- Centralize versions for common dependencies [\#235](https://github.com/stargate/stargate/pull/235) ([olim7t](https://github.com/olim7t))
- Re-add build step: Cache Maven packages [\#229](https://github.com/stargate/stargate/pull/229) ([dimas-b](https://github.com/dimas-b))
- Add GraphQL unit test example for DML queries [\#228](https://github.com/stargate/stargate/pull/228) ([olim7t](https://github.com/olim7t))
- Add testing instructions to DEV\_GUIDE.md [\#227](https://github.com/stargate/stargate/pull/227) ([dimas-b](https://github.com/dimas-b))
- Support list, set and map data types in GraphQL [\#225](https://github.com/stargate/stargate/pull/225) ([jorgebay](https://github.com/jorgebay))
- Bump guava on tests [\#223](https://github.com/stargate/stargate/pull/223) ([jorgebay](https://github.com/jorgebay))
- Clean up GraphQL schema code [\#221](https://github.com/stargate/stargate/pull/221) ([olim7t](https://github.com/olim7t))
- Fix `UnauthorizedException` errors [\#219](https://github.com/stargate/stargate/pull/219) ([mpenick](https://github.com/mpenick))
- Only use dse profile in CI for dse backend tests [\#218](https://github.com/stargate/stargate/pull/218) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Use a profile to build DSE [\#217](https://github.com/stargate/stargate/pull/217) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Use longer driver timeouts during tests [\#215](https://github.com/stargate/stargate/pull/215) ([dimas-b](https://github.com/dimas-b))
- Per module persistence integration tests [\#214](https://github.com/stargate/stargate/pull/214) ([dimas-b](https://github.com/dimas-b))
- Add logback config for integration tests [\#212](https://github.com/stargate/stargate/pull/212) ([olim7t](https://github.com/olim7t))
- Add Documents API [\#211](https://github.com/stargate/stargate/pull/211) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version for next release [\#201](https://github.com/stargate/stargate/pull/201) ([github-actions[bot]](https://github.com/apps/github-actions))
- Avoid accessing java service interfaces in backbox tests [\#198](https://github.com/stargate/stargate/pull/198) ([dimas-b](https://github.com/dimas-b))
- Use default value for Clustering Key [\#196](https://github.com/stargate/stargate/pull/196) ([jorgebay](https://github.com/jorgebay))
- Add more CQL tests [\#169](https://github.com/stargate/stargate/pull/169) ([olim7t](https://github.com/olim7t))
- Add swagger support for restapi [\#168](https://github.com/stargate/stargate/pull/168) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.14](https://github.com/stargate/stargate/tree/v0.0.14) (2020-10-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.13...v0.0.14)

**Merged pull requests:**

- Bumping version for next release [\#200](https://github.com/stargate/stargate/pull/200) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v0.0.13](https://github.com/stargate/stargate/tree/v0.0.13) (2020-10-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.12...v0.0.13)

**Closed issues:**

- CQL API does not surface query warnings [\#180](https://github.com/stargate/stargate/issues/180)

**Merged pull requests:**

- Remove default heap and fix use-proxy-protocol arg in starctl [\#199](https://github.com/stargate/stargate/pull/199) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix NPE when converting `TruncateException` [\#194](https://github.com/stargate/stargate/pull/194) ([mpenick](https://github.com/mpenick))
- Run integration tests in parallel in CI [\#193](https://github.com/stargate/stargate/pull/193) ([dimas-b](https://github.com/dimas-b))
- Fix already exists errors [\#192](https://github.com/stargate/stargate/pull/192) ([mpenick](https://github.com/mpenick))
- Fix errors for invalid amount of bind variables [\#190](https://github.com/stargate/stargate/pull/190) ([mpenick](https://github.com/mpenick))
- Bumping version for next release [\#189](https://github.com/stargate/stargate/pull/189) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v0.0.12](https://github.com/stargate/stargate/tree/v0.0.12) (2020-10-02)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.11...v0.0.12)

**Closed issues:**

- Allow users to createKeyspace with GraphQL [\#155](https://github.com/stargate/stargate/issues/155)

**Merged pull requests:**

- Unwrap protocol errors [\#186](https://github.com/stargate/stargate/pull/186) ([jorgebay](https://github.com/jorgebay))
- Fix dse bind by name [\#183](https://github.com/stargate/stargate/pull/183) ([mpenick](https://github.com/mpenick))
- Run integration tests against DSE 6.8.4 [\#182](https://github.com/stargate/stargate/pull/182) ([dimas-b](https://github.com/dimas-b))
- Fix native protocol version negotiation for DSE persistence [\#181](https://github.com/stargate/stargate/pull/181) ([mpenick](https://github.com/mpenick))
- Bind DSE JMX server to listen address [\#179](https://github.com/stargate/stargate/pull/179) ([jorgebay](https://github.com/jorgebay))
- Fix handling of unset values [\#178](https://github.com/stargate/stargate/pull/178) ([pcmanus](https://github.com/pcmanus))
- Handle null pagingState in DSE persistence [\#175](https://github.com/stargate/stargate/pull/175) ([dimas-b](https://github.com/dimas-b))
- Run integration tests against C\* 4.0 [\#174](https://github.com/stargate/stargate/pull/174) ([dimas-b](https://github.com/dimas-b))
- Fix DSE persistence [\#173](https://github.com/stargate/stargate/pull/173) ([mpenick](https://github.com/mpenick))
- Add sun.rmi.registry to felix packages for jmx support [\#170](https://github.com/stargate/stargate/pull/170) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix building of QueryOptions ignoring some options [\#167](https://github.com/stargate/stargate/pull/167) ([pcmanus](https://github.com/pcmanus))
- Fix Varint conversion [\#166](https://github.com/stargate/stargate/pull/166) ([tlasica](https://github.com/tlasica))
- Add GraphQL createKeyspace mutation \(fixes \#155\) [\#165](https://github.com/stargate/stargate/pull/165) ([olim7t](https://github.com/olim7t))
- new stargate-modules diagram with graphql [\#163](https://github.com/stargate/stargate/pull/163) ([polandll](https://github.com/polandll))
- Include extentions [\#162](https://github.com/stargate/stargate/pull/162) ([dimas-b](https://github.com/dimas-b))
- Don't build iterable from stream with `\#iterator` [\#161](https://github.com/stargate/stargate/pull/161) ([pcmanus](https://github.com/pcmanus))
- Add basic request metrics for GraphQL [\#158](https://github.com/stargate/stargate/pull/158) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#157](https://github.com/stargate/stargate/pull/157) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add core module [\#152](https://github.com/stargate/stargate/pull/152) ([jorgebay](https://github.com/jorgebay))
- Create integration test that with recommended driver settings Stargate nodes are uniformly loaded with requests [\#93](https://github.com/stargate/stargate/pull/93) ([tomekl007](https://github.com/tomekl007))

## [v0.0.11](https://github.com/stargate/stargate/tree/v0.0.11) (2020-09-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.10...v0.0.11)

**Closed issues:**

- Standardize unit testing framework [\#94](https://github.com/stargate/stargate/issues/94)

**Merged pull requests:**

- Wait for schema migration to occur during startup [\#160](https://github.com/stargate/stargate/pull/160) ([mpenick](https://github.com/mpenick))
- Convert BaseStorageIntegrationTest to a JUnit 5 extension [\#156](https://github.com/stargate/stargate/pull/156) ([dimas-b](https://github.com/dimas-b))
- Use surefire for unit tests [\#153](https://github.com/stargate/stargate/pull/153) ([jorgebay](https://github.com/jorgebay))
- Cleanup of schema handling in persistence-api [\#151](https://github.com/stargate/stargate/pull/151) ([pcmanus](https://github.com/pcmanus))
- Revert making the GraphQL data fetchers async [\#150](https://github.com/stargate/stargate/pull/150) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#149](https://github.com/stargate/stargate/pull/149) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add missing licenses in GraphQL module [\#148](https://github.com/stargate/stargate/pull/148) ([olim7t](https://github.com/olim7t))
- REST/v1 with compound partition key /rows/{pk} of mixed types leads to 400 [\#117](https://github.com/stargate/stargate/pull/117) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix handling of partition and clustering keys in REST api [\#116](https://github.com/stargate/stargate/pull/116) ([dougwettlaufer](https://github.com/dougwettlaufer))
- /keyspaces/{ks}/tables/{t}/columns fails with 500 [\#99](https://github.com/stargate/stargate/pull/99) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.10](https://github.com/stargate/stargate/tree/v0.0.10) (2020-09-24)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.9...v0.0.10)

**Merged pull requests:**

- Use JUnit 5 for testing [\#146](https://github.com/stargate/stargate/pull/146) ([dimas-b](https://github.com/dimas-b))
- Use ccm for managing test cluster during IT runs  [\#114](https://github.com/stargate/stargate/pull/114) ([dimas-b](https://github.com/dimas-b))
- Bumping version for next release [\#113](https://github.com/stargate/stargate/pull/113) ([github-actions[bot]](https://github.com/apps/github-actions))
- Adding graphql module [\#13](https://github.com/stargate/stargate/pull/13) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.9](https://github.com/stargate/stargate/tree/v0.0.9) (2020-09-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.8...v0.0.9)

**Closed issues:**

- http POST http://$STARGATE:8081/v1/auth without body results in 500 and NPE [\#55](https://github.com/stargate/stargate/issues/55)
- REST v1 /query  errors 500 when unexpected JSON data key is used [\#52](https://github.com/stargate/stargate/issues/52)

**Merged pull requests:**

- Set heap in starctl [\#112](https://github.com/stargate/stargate/pull/112) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add endpoint for token by username [\#111](https://github.com/stargate/stargate/pull/111) ([dougwettlaufer](https://github.com/dougwettlaufer))
- CQL: Add token-based authentication [\#42](https://github.com/stargate/stargate/pull/42) ([mpenick](https://github.com/mpenick))
- Setup sonar [\#14](https://github.com/stargate/stargate/pull/14) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.8](https://github.com/stargate/stargate/tree/v0.0.8) (2020-09-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.7...v0.0.8)

**Closed issues:**

- Stargate metrics [\#69](https://github.com/stargate/stargate/issues/69)

**Merged pull requests:**

- Move timestamp binding logic over to persistence-api [\#109](https://github.com/stargate/stargate/pull/109) ([EricBorczuk](https://github.com/EricBorczuk))
- Skip xml formatting for now since it's way to finicky [\#108](https://github.com/stargate/stargate/pull/108) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Fix pom.xml formatting so that build is not broken [\#107](https://github.com/stargate/stargate/pull/107) ([tlasica](https://github.com/tlasica))
- Make initializing tablebased auth configurable [\#106](https://github.com/stargate/stargate/pull/106) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Run mvn fmt on release [\#105](https://github.com/stargate/stargate/pull/105) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bumping version for next release [\#104](https://github.com/stargate/stargate/pull/104) ([github-actions[bot]](https://github.com/apps/github-actions))
- REST v1 /query  errors 500 when unexpected JSON data key is used [\#103](https://github.com/stargate/stargate/pull/103) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Remove pretty param [\#102](https://github.com/stargate/stargate/pull/102) ([dougwettlaufer](https://github.com/dougwettlaufer))
- POST to /v1/auth without any params results in 500 and NPE [\#98](https://github.com/stargate/stargate/pull/98) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add health-checker module and centralize metrics [\#39](https://github.com/stargate/stargate/pull/39) ([olim7t](https://github.com/olim7t))

## [v0.0.7](https://github.com/stargate/stargate/tree/v0.0.7) (2020-09-17)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.6...v0.0.7)

**Merged pull requests:**

- Bumping version for next release [\#97](https://github.com/stargate/stargate/pull/97) ([github-actions[bot]](https://github.com/apps/github-actions))
- Adjust pom to sign correct jar on release [\#96](https://github.com/stargate/stargate/pull/96) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Enforce Java and XML source formatting [\#91](https://github.com/stargate/stargate/pull/91) ([olim7t](https://github.com/olim7t))

## [v0.0.6](https://github.com/stargate/stargate/tree/v0.0.6) (2020-09-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.5...v0.0.6)

**Closed issues:**

- Consider defaulting to ScyllaDB over Cassandara [\#87](https://github.com/stargate/stargate/issues/87)
- Add integration tests for CQL [\#73](https://github.com/stargate/stargate/issues/73)
- Stargate should not waste memory on ChunkCache \(or other default storage related buffers\) [\#67](https://github.com/stargate/stargate/issues/67)
- CQL: Page state is not set in response [\#53](https://github.com/stargate/stargate/issues/53)

**Merged pull requests:**

- Fix plugins for maven release [\#92](https://github.com/stargate/stargate/pull/92) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add server-id to setup-java step [\#90](https://github.com/stargate/stargate/pull/90) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Remove last GH packages reference [\#89](https://github.com/stargate/stargate/pull/89) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Switch release to maven central [\#88](https://github.com/stargate/stargate/pull/88) ([dougwettlaufer](https://github.com/dougwettlaufer))
- C2-258: Only fetching first page with C\* 3.11 backend && C2-281: CQL: Page state is not set in response [\#66](https://github.com/stargate/stargate/pull/66) ([tomekl007](https://github.com/tomekl007))
- Update README.md [\#47](https://github.com/stargate/stargate/pull/47) ([csplinter](https://github.com/csplinter))
- More CQL tests [\#46](https://github.com/stargate/stargate/pull/46) ([jorgebay](https://github.com/jorgebay))
- Fix restv2 test after bad merge [\#44](https://github.com/stargate/stargate/pull/44) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Don't clean stargate-lib/logback.xml [\#43](https://github.com/stargate/stargate/pull/43) ([olim7t](https://github.com/olim7t))
- Move `system.local` and `system.peers` handling into an interface [\#41](https://github.com/stargate/stargate/pull/41) ([mpenick](https://github.com/mpenick))
- fix start ccm script [\#40](https://github.com/stargate/stargate/pull/40) ([tomekl007](https://github.com/tomekl007))
- Update rest and auth apis [\#38](https://github.com/stargate/stargate/pull/38) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Handle missing token on requests [\#37](https://github.com/stargate/stargate/pull/37) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Create PR instead of commit since master is protected [\#36](https://github.com/stargate/stargate/pull/36) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Rev version 0.0.6-SNAPSHOT [\#35](https://github.com/stargate/stargate/pull/35) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Remove coordinator and filterchain [\#31](https://github.com/stargate/stargate/pull/31) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add logback.xml [\#30](https://github.com/stargate/stargate/pull/30) ([dougwettlaufer](https://github.com/dougwettlaufer))
- add NOTICE, licenses-report, license headers [\#28](https://github.com/stargate/stargate/pull/28) ([csplinter](https://github.com/csplinter))
- Create CODE\_OF\_CONDUCT.md [\#20](https://github.com/stargate/stargate/pull/20) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Update README.md [\#17](https://github.com/stargate/stargate/pull/17) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.5](https://github.com/stargate/stargate/tree/v0.0.5) (2020-09-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.4...v0.0.5)

**Merged pull requests:**

- Update release.yml [\#34](https://github.com/stargate/stargate/pull/34) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Commit to master on release action [\#33](https://github.com/stargate/stargate/pull/33) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Support developer mode [\#32](https://github.com/stargate/stargate/pull/32) ([jorgebay](https://github.com/jorgebay))

## [v0.0.4](https://github.com/stargate/stargate/tree/v0.0.4) (2020-09-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.3...v0.0.4)

**Merged pull requests:**

- Use maven to handle pom versions rather than property [\#29](https://github.com/stargate/stargate/pull/29) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Readme enhancements [\#25](https://github.com/stargate/stargate/pull/25) ([csplinter](https://github.com/csplinter))

## [v0.0.3](https://github.com/stargate/stargate/tree/v0.0.3) (2020-09-11)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.2...v0.0.3)

**Closed issues:**

- testing integrations [\#27](https://github.com/stargate/stargate/issues/27)
- StarterTest - SetStargatePropertiesWithBadHostSeedNode [\#26](https://github.com/stargate/stargate/issues/26)
- testing pom.xml has hardcoded version [\#23](https://github.com/stargate/stargate/issues/23)
- CCM command on mac issues [\#19](https://github.com/stargate/stargate/issues/19)

**Merged pull requests:**

- Add a settings.xml when deploying to GH packages [\#24](https://github.com/stargate/stargate/pull/24) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Create CONTRIBUTING.md [\#22](https://github.com/stargate/stargate/pull/22) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.2](https://github.com/stargate/stargate/tree/v0.0.2) (2020-09-10)

[Full Changelog](https://github.com/stargate/stargate/compare/v0.0.1...v0.0.2)

**Merged pull requests:**

- Add REST module [\#12](https://github.com/stargate/stargate/pull/12) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Update readme [\#11](https://github.com/stargate/stargate/pull/11) ([dougwettlaufer](https://github.com/dougwettlaufer))

## [v0.0.1](https://github.com/stargate/stargate/tree/v0.0.1) (2020-09-10)

[Full Changelog](https://github.com/stargate/stargate/compare/439bb88c9cd38c296700c07a018c8b92ca669a38...v0.0.1)

**Merged pull requests:**

- Update CODEOWNERS [\#21](https://github.com/stargate/stargate/pull/21) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Bind http services to the listen address [\#18](https://github.com/stargate/stargate/pull/18) ([jorgebay](https://github.com/jorgebay))
- Add testing module [\#16](https://github.com/stargate/stargate/pull/16) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add maven-publish workflow [\#15](https://github.com/stargate/stargate/pull/15) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add core auth modules [\#10](https://github.com/stargate/stargate/pull/10) ([dougwettlaufer](https://github.com/dougwettlaufer))
- CQL and supporting modules [\#9](https://github.com/stargate/stargate/pull/9) ([mpenick](https://github.com/mpenick))
- Change duzzt dependency [\#8](https://github.com/stargate/stargate/pull/8) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add start script [\#7](https://github.com/stargate/stargate/pull/7) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Move ci workflow to correct directory [\#6](https://github.com/stargate/stargate/pull/6) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Create LICENSE [\#5](https://github.com/stargate/stargate/pull/5) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add persistence modules [\#4](https://github.com/stargate/stargate/pull/4) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add CI action to build commits [\#3](https://github.com/stargate/stargate/pull/3) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Add stargate-starter module [\#2](https://github.com/stargate/stargate/pull/2) ([dougwettlaufer](https://github.com/dougwettlaufer))
- Create CODEOWNERS [\#1](https://github.com/stargate/stargate/pull/1) ([dougwettlaufer](https://github.com/dougwettlaufer))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
