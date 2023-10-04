# Changelog

## [v2.1.0-BETA-1](https://github.com/stargate/stargate/tree/v2.1.0-BETA-1) (2023-10-03)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-11...v2.1.0-BETA-1)

**Closed issues:**

- Update Quarkus from 3.2.1 to 3.3.6, align `grpc-core` version to `1.56.1` [\#2802](https://github.com/stargate/stargate/issues/2802)

**Merged pull requests:**

- Merge fix from C-4 EncyptionOptions to Stargate cql module, to remove eager fail [\#2793](https://github.com/stargate/stargate/pull/2793) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove unnecessary UserDefinedFunctionHelper.fixCompilerClassLoader\(\) from C-4.1 backend [\#2792](https://github.com/stargate/stargate/pull/2792) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update dse-next/dse-db-all to latest as of 2023-09-27: 4.0.11-669ae5e3994d [\#2790](https://github.com/stargate/stargate/pull/2790) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#2781: remove Duzzt-library/processor, to allow for easier upgrade from JDK 11 [\#2787](https://github.com/stargate/stargate/pull/2787) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-11](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-11) (2023-09-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-10...v2.1.0-ALPHA-11)

**Closed issues:**

- Update to DSE 6.8.38 [\#2767](https://github.com/stargate/stargate/issues/2767)
- Update to latest `java-driver-core`, 4.17.0 \(from 4.15.0/4.14.1\) [\#2765](https://github.com/stargate/stargate/issues/2765)
- Log Quarkus API Request Body for debugging [\#2730](https://github.com/stargate/stargate/issues/2730)
- Update to DSE 6.8.37 [\#2667](https://github.com/stargate/stargate/issues/2667)

**Merged pull requests:**

- Or support in `QueryBuilder` [\#2778](https://github.com/stargate/stargate/pull/2778) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Manual merge of v2 \(main\) to v21 -- GH Action for creating PR failed due to conflicts [\#2777](https://github.com/stargate/stargate/pull/2777) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update `dse-next` to use latest `dse-db-all`, `4.0.7 0cf63a3d0b6d` [\#2771](https://github.com/stargate/stargate/pull/2771) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Merge changes wrt \#2765 from main \(v2\) to v2.1 [\#2768](https://github.com/stargate/stargate/pull/2768) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next v2.1 release [\#2764](https://github.com/stargate/stargate/pull/2764) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v2.1.0-ALPHA-10](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-10) (2023-09-08)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-9...v2.1.0-ALPHA-10)

## [v2.1.0-ALPHA-9](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-9) (2023-08-31)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-8...v2.1.0-ALPHA-9)

**Merged pull requests:**

- revert cql protocol v5 [\#2751](https://github.com/stargate/stargate/pull/2751) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v2.1.0-ALPHA-8](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-8) (2023-08-31)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-7...v2.1.0-ALPHA-8)

**Closed issues:**

- Update "dse-next" to latest backing data store version [\#2745](https://github.com/stargate/stargate/issues/2745)
- Add tests for basic Vector operations via `/cql` endpoint [\#2731](https://github.com/stargate/stargate/issues/2731)
- Add tests for fail/edge cases of Vector+REST CRUD operations [\#2715](https://github.com/stargate/stargate/issues/2715)
- Fix ITs for DSE Next Persistence [\#2685](https://github.com/stargate/stargate/issues/2685)

**Merged pull requests:**

- Update dse-db-all to 4.0.7-e47eb8e14b96 [\#2750](https://github.com/stargate/stargate/pull/2750) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bump aws-actions/configure-aws-credentials from 2 to 3 [\#2749](https://github.com/stargate/stargate/pull/2749) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update `dse-db-all` dependency of `dse-next` backend to latest \(hash 4baff43cf3ae\) [\#2746](https://github.com/stargate/stargate/pull/2746) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add tests for REST/Vector/CRUD edge cases [\#2744](https://github.com/stargate/stargate/pull/2744) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Cassandra-4.1 backend for SGv 2.1 [\#2711](https://github.com/stargate/stargate/pull/2711) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-7](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-7) (2023-08-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-6...v2.1.0-ALPHA-7)

**Closed issues:**

- Add GH workflow for creating PR for merging changes from 2.0 \(`main`\) to 2.1 \(`v2.1`\) [\#2734](https://github.com/stargate/stargate/issues/2734)
- REST API swagger error reg compactMapData flag [\#2733](https://github.com/stargate/stargate/issues/2733)
- Log DocsAPI Batch extrapolation size [\#2728](https://github.com/stargate/stargate/issues/2728)
- Support vector search similarity function in the bridge. [\#2714](https://github.com/stargate/stargate/issues/2714)
- Add tests for `dse-next` backend to verify basic Vector functionality over gRPC [\#2704](https://github.com/stargate/stargate/issues/2704)
- Add vector support to gRPC API  [\#2654](https://github.com/stargate/stargate/issues/2654)
- Add vector CRUD support to REST API, happy path tests [\#2653](https://github.com/stargate/stargate/issues/2653)
- Separate comparable bytes and resume mode on the bridge [\#1967](https://github.com/stargate/stargate/issues/1967)

**Merged pull requests:**

- Add ITs for Vector operations via /cql endpoint [\#2740](https://github.com/stargate/stargate/pull/2740) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Need to fix reference to base for PR \(wrt \#2734\) [\#2738](https://github.com/stargate/stargate/pull/2738) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix naming of workflow to have .yaml suffix \(not recognized otherwise\) [\#2737](https://github.com/stargate/stargate/pull/2737) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add new GH workflow action for creating PR to merge main \(v2\) to v2.1… [\#2736](https://github.com/stargate/stargate/pull/2736) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- 'compactMapData' param added to the parameters definition [\#2735](https://github.com/stargate/stargate/pull/2735) ([kathirsvn](https://github.com/kathirsvn))
- Fix `dse-next` persistence backend to return `true` for `supportsSAI\(\)` [\#2732](https://github.com/stargate/stargate/pull/2732) ([Yuqi-Du](https://github.com/Yuqi-Du))
- DocsAPI: Debug log to log the batch size when a doc is translated  [\#2729](https://github.com/stargate/stargate/pull/2729) ([kathirsvn](https://github.com/kathirsvn))
- update the protobuf version in the bridge-proto pom file [\#2727](https://github.com/stargate/stargate/pull/2727) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Error msg vector codec [\#2726](https://github.com/stargate/stargate/pull/2726) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Jeff/v2.1/system keyspace [\#2725](https://github.com/stargate/stargate/pull/2725) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add vector support to gRPC API [\#2722](https://github.com/stargate/stargate/pull/2722) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix \#2653: add vector search CRUD support for REST API [\#2706](https://github.com/stargate/stargate/pull/2706) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-6](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-6) (2023-08-08)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-5...v2.1.0-ALPHA-6)

**Closed issues:**

- Quarkus-common library `QueryBuilderImpl` incorrectly quotes size parameter of Vector type [\#2712](https://github.com/stargate/stargate/issues/2712)
- Add array size validation in bridge vector codec [\#2707](https://github.com/stargate/stargate/issues/2707)
- Add CQL tests for `dse-next` backend to verify basic Vector functionality over cql [\#2698](https://github.com/stargate/stargate/issues/2698)
- Ensure `RowDecoratorImpl` for `persistence-dse-next` uses impl compatible with the backend [\#2693](https://github.com/stargate/stargate/issues/2693)
- Create "DSE next" persistence module  [\#2663](https://github.com/stargate/stargate/issues/2663)
- Integration test failure in CI due to failure to connect to Docker engine [\#1580](https://github.com/stargate/stargate/issues/1580)

**Merged pull requests:**

- Similarity functions [\#2718](https://github.com/stargate/stargate/pull/2718) ([maheshrajamani](https://github.com/maheshrajamani))
- cassandra 4.0 docker compose updates [\#2716](https://github.com/stargate/stargate/pull/2716) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#2712: make QueryBuilder avoid quoting numbers [\#2713](https://github.com/stargate/stargate/pull/2713) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Vector size validation [\#2708](https://github.com/stargate/stargate/pull/2708) ([maheshrajamani](https://github.com/maheshrajamani))
- Add `size\(\)` method for `ColumnType`, implemented by `VectorType` [\#2700](https://github.com/stargate/stargate/pull/2700) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add Vector-via-cql tests for Coordinator [\#2699](https://github.com/stargate/stargate/pull/2699) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-5](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-5) (2023-07-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-4...v2.1.0-ALPHA-5)

**Merged pull requests:**

- Update `dse-db-all` jar to latest as of 2023-07-25 \(4.0.7-336cdd7405ee\) [\#2695](https://github.com/stargate/stargate/pull/2695) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-4](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-4) (2023-07-219)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-3...v2.1.0-ALPHA-4)

## [v2.1.0-ALPHA-3](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-3) (2023-07-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-2...v2.1.0-ALPHA-3)

**Closed issues:**

- REST Get All Indexes Test fix [\#2687](https://github.com/stargate/stargate/issues/2687)
- Shade "cassandra-all" dependency by `cql` module [\#2680](https://github.com/stargate/stargate/issues/2680)
- Table already exists 500 instead of 409 [\#2679](https://github.com/stargate/stargate/issues/2679)
- REST Timestamp with additional fraction not accepted [\#2674](https://github.com/stargate/stargate/issues/2674)
- Remove SERVER\_VERSION and PRODUCT\_TYPE assertions [\#2599](https://github.com/stargate/stargate/issues/2599)
- Document exposed system properties [\#1365](https://github.com/stargate/stargate/issues/1365)
- Remove `cassandra-all` dependency from CQL transport [\#660](https://github.com/stargate/stargate/issues/660)

**Merged pull requests:**

- Vector search feature support flag [\#2686](https://github.com/stargate/stargate/pull/2686) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#2679: handle dup table creation via REST [\#2684](https://github.com/stargate/stargate/pull/2684) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix 2680: shade cassandra-all 4.0 dep by cql module [\#2683](https://github.com/stargate/stargate/pull/2683) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Vector search integration to Stargate bridge  [\#2678](https://github.com/stargate/stargate/pull/2678) ([maheshrajamani](https://github.com/maheshrajamani))
- Reduce Guava usage/dependencies to help against transitive Guava dep conflicts [\#2677](https://github.com/stargate/stargate/pull/2677) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#2674: support microsecond fractions for ISO-8601 date/time for REST API [\#2675](https://github.com/stargate/stargate/pull/2675) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-ALPHA-2](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-2) (2023-07-12)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-ALPHA-1...v2.1.0-ALPHA-2)

**Closed issues:**

- Support protocol v5  [\#770](https://github.com/stargate/stargate/issues/770)

**Merged pull requests:**

- fix docker image tagging for apis [\#2672](https://github.com/stargate/stargate/pull/2672) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Enable HAProxy if configured [\#2670](https://github.com/stargate/stargate/pull/2670) ([maheshrajamani](https://github.com/maheshrajamani))

## [v2.1.0-ALPHA-1](https://github.com/stargate/stargate/tree/v2.1.0-ALPHA-1) (2023-07-04)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.16...v2.1.0-ALPHA-1)

**Closed issues:**

- Guava version 32.0.0 has a CVE [\#2638](https://github.com/stargate/stargate/issues/2638)
- Enable Github Action based Coordinator Test to replace GCB for Stargate V2 [\#2281](https://github.com/stargate/stargate/issues/2281)
- Remove in-coordinator \(SGv1\) RESTv2 API \(leave RESTv1\) [\#2121](https://github.com/stargate/stargate/issues/2121)
- Remove in-coordinator \(SGv1\) GraphQL-api [\#2117](https://github.com/stargate/stargate/issues/2117)

**Merged pull requests:**

- docker image tagging for v2.1 [\#2655](https://github.com/stargate/stargate/pull/2655) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- v2.1 release workflow [\#2652](https://github.com/stargate/stargate/pull/2652) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update to Quarkus 3.1.3, sync grpc versions [\#2650](https://github.com/stargate/stargate/pull/2650) ([ivansenic](https://github.com/ivansenic))
- Add JSON as an API option [\#2644](https://github.com/stargate/stargate/pull/2644) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix DSE ITs for Java 11 \(second attempt\) [\#2643](https://github.com/stargate/stargate/pull/2643) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Remove rate limiting [\#2641](https://github.com/stargate/stargate/pull/2641) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Remove REST and GraphQL APIs from coordinator [\#2640](https://github.com/stargate/stargate/pull/2640) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix GH actions to run for v2.1 too [\#2634](https://github.com/stargate/stargate/pull/2634) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove unnecessary maven-failsafe-plugin \(IT\) settings [\#2632](https://github.com/stargate/stargate/pull/2632) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix formatting error [\#2630](https://github.com/stargate/stargate/pull/2630) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- add java exports to stargate extension cmd line [\#2629](https://github.com/stargate/stargate/pull/2629) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Try to fix v2.1 branch ITs [\#2628](https://github.com/stargate/stargate/pull/2628) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update version to 2.1.0-ALPHA-1-SNAPSHOT for v2.1 branch [\#2625](https://github.com/stargate/stargate/pull/2625) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- removing Cassandra 3.11 support [\#2614](https://github.com/stargate/stargate/pull/2614) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- JDK 11 upgrade work, prep for vector-search feature [\#2613](https://github.com/stargate/stargate/pull/2613) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Protocol v5 support for cql [\#2611](https://github.com/stargate/stargate/pull/2611) ([maheshrajamani](https://github.com/maheshrajamani))

