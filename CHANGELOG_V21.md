# Changelog

## [Unreleased](https://github.com/stargate/stargate/tree/HEAD)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-BETA-5...HEAD)

**Closed issues:**

- Update "dse-next" to latest available version `4.0.11-7202bd32beef` \(2023-12-04\) [\#2850](https://github.com/stargate/stargate/issues/2850)

## [v2.1.0-BETA-5](https://github.com/stargate/stargate/tree/v2.1.0-BETA-5) (2023-12-04)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-BETA-4...v2.1.0-BETA-5)

**Closed issues:**

- Update "dse-next" to latest version from `cndb/vsearch` \(2023-12-01\) [\#2846](https://github.com/stargate/stargate/issues/2846)

**Merged pull requests:**

- Fix \#2850: dse-next to `4.0.11-7202bd32beef` [\#2852](https://github.com/stargate/stargate/pull/2852) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next v2.1 release [\#2848](https://github.com/stargate/stargate/pull/2848) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v2.1.0-BETA-4](https://github.com/stargate/stargate/tree/v2.1.0-BETA-4) (2023-12-01)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-BETA-3...v2.1.0-BETA-4)

**Closed issues:**

- Update "dse-next" to latest version from `cndb/vsearch` \(2023-12-01\) [\#2846](https://github.com/stargate/stargate/issues/2846)
- Update "dse-next" to latest version from `cndb/vsearch` \(2023-11-13\) [\#2834](https://github.com/stargate/stargate/issues/2834)
- Update to DSE 6.8.40 [\#2833](https://github.com/stargate/stargate/issues/2833)
- Update logback to `1.3.0` [\#1281](https://github.com/stargate/stargate/issues/1281)

**Merged pull requests:**

- Fix \#2846: import latest "dse-db-all" for `dse-next` backend [\#2847](https://github.com/stargate/stargate/pull/2847) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bump ch.qos.logback:logback-classic from 1.3.11 to 1.3.13 in /coordinator [\#2845](https://github.com/stargate/stargate/pull/2845) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump com.squareup.okio:okio-jvm from 3.0.0 to 3.4.0 in /coordinator/testing [\#2841](https://github.com/stargate/stargate/pull/2841) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix \#1281: update to Logback 1.3.x \(from 1.2.x\); slf4j-api to 2.0 \(from 1.3\) [\#2839](https://github.com/stargate/stargate/pull/2839) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update DSE-6.8 backend to 6.8.40 [\#2837](https://github.com/stargate/stargate/pull/2837) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Update dse-db-all dep by dse-next to "4.0.11-49a1eecaf3e4" [\#2835](https://github.com/stargate/stargate/pull/2835) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.1.0-BETA-3](https://github.com/stargate/stargate/tree/v2.1.0-BETA-3) (2023-11-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-BETA-2...v2.1.0-BETA-3)

**Closed issues:**

- Update to DSE 6.8.39 [\#2810](https://github.com/stargate/stargate/issues/2810)

**Merged pull requests:**

- Fix Python 3.12/ccm/CI issue [\#2831](https://github.com/stargate/stargate/pull/2831) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2810: update DSE-6.8 backed to 6.8.39 [\#2829](https://github.com/stargate/stargate/pull/2829) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Added stargate cql port to properties exported [\#2827](https://github.com/stargate/stargate/pull/2827) ([maheshrajamani](https://github.com/maheshrajamani))
- Update to dse-db-all 4.0.11-45d4657e507e \(2023-10-23\) [\#2820](https://github.com/stargate/stargate/pull/2820) ([tatu-at-datastax](https://github.com/tatu-at-datastax))


## [v2.1.0-BETA-2](https://github.com/stargate/stargate/tree/v2.1.0-BETA-2) (2023-10-20)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.1.0-BETA-1...v2.1.0-BETA-2)

**Implemented enhancements:**

- Add helper method for logging in Starter.java [\#1303](https://github.com/stargate/stargate/issues/1303)
- Introduce a schema migration feature for Stargate [\#1295](https://github.com/stargate/stargate/issues/1295)
- Array-based filter path.\[\*\] returns empty data [\#1247](https://github.com/stargate/stargate/issues/1247)
- Support `$exists` for the object nodes, not only leaves [\#1133](https://github.com/stargate/stargate/issues/1133)
- Optionally provide further info about query execution in REST API [\#823](https://github.com/stargate/stargate/issues/823)

**Closed issues:**

- add our own logic expression classes [\#2812](https://github.com/stargate/stargate/issues/2812)
- Create v2.1 changelog [\#2720](https://github.com/stargate/stargate/issues/2720)
- Revisit memory allocation in docker compose scripts [\#2607](https://github.com/stargate/stargate/issues/2607)
- How does CQL respond when there is an error on gRPC interaction? [\#2452](https://github.com/stargate/stargate/issues/2452)
- Fix changelog generation [\#2435](https://github.com/stargate/stargate/issues/2435)
- Document how to enable SSL on Stargate v2 APIs [\#2381](https://github.com/stargate/stargate/issues/2381)
- Remove broadcast\_rpc\_address from sample cql.yaml [\#2180](https://github.com/stargate/stargate/issues/2180)
- Native image for GraphQL API Service native image [\#2156](https://github.com/stargate/stargate/issues/2156)
- Native image for REST API Service native image [\#2155](https://github.com/stargate/stargate/issues/2155)
- Add `JAVA\_OPTS\_APPEND` \(or similar\) env variable for docker-compose scripts to pass to services [\#2154](https://github.com/stargate/stargate/issues/2154)
- Stargate bibtex for citation [\#2057](https://github.com/stargate/stargate/issues/2057)
- Define maximum `page-size` in `stargate\_claims` in JWT [\#2049](https://github.com/stargate/stargate/issues/2049)
- Optimistic schema for the Docs API V2 [\#2007](https://github.com/stargate/stargate/issues/2007)
- gRPC Dart client [\#2005](https://github.com/stargate/stargate/issues/2005)
- Add `ttlAuto` for the `$push` and `$pop` functions [\#1974](https://github.com/stargate/stargate/issues/1974)
- Add support for retires in Docs API V2 [\#1971](https://github.com/stargate/stargate/issues/1971)
- Improve `WriteDocumentsServiceTest` by decreasing mocks [\#1956](https://github.com/stargate/stargate/issues/1956)
- Native image and end-to-end test support in Document V2 [\#1955](https://github.com/stargate/stargate/issues/1955)
- Allow multiple field matching for a single document [\#1935](https://github.com/stargate/stargate/issues/1935)
- Make materialized views active by default or expose control in envrionment variable [\#1925](https://github.com/stargate/stargate/issues/1925)
- Integration test `it.test=io.stargate.it.bridge.SchemaTest\#describeKeyspace` can fail the `isCustom\(\)` index check [\#1919](https://github.com/stargate/stargate/issues/1919)
- Revisit timestamp generation in Documents v2 API [\#1850](https://github.com/stargate/stargate/issues/1850)
- HTTP APIs should report `RequestTimeoutException` as `504 Gateway Timeout` [\#1667](https://github.com/stargate/stargate/issues/1667)
- Submit CSV to CQL [\#1512](https://github.com/stargate/stargate/issues/1512)
- Pre-process DML data before binding the values [\#1494](https://github.com/stargate/stargate/issues/1494)
- Upgrade test infra to allow custom storage configuration [\#1356](https://github.com/stargate/stargate/issues/1356)
- Refactor private copies of `slf4j-api`, `logback` from bundles into shared OSGi context [\#1345](https://github.com/stargate/stargate/issues/1345)
- Add missing unit tests for the `QueryBuilderImpl` [\#1296](https://github.com/stargate/stargate/issues/1296)
- Backport `NonBlockingRateLimiter` to the CQL module [\#1254](https://github.com/stargate/stargate/issues/1254)
- Bulk update and delete mutations in GraphQL API [\#1223](https://github.com/stargate/stargate/issues/1223)
- REST API and DocsAPI should both handle `fields` param in the same way [\#1213](https://github.com/stargate/stargate/issues/1213)
- Retrofit CQL-first mappers to use MutationPayload [\#1207](https://github.com/stargate/stargate/issues/1207)
- Add a new parameter `sort` in the `searchDoc` method in order to sort the results [\#1205](https://github.com/stargate/stargate/issues/1205)
- Refactor CQL-first schema builders to avoid deprecated method [\#1201](https://github.com/stargate/stargate/issues/1201)
- Revert azure-storage-blob exclusion [\#1200](https://github.com/stargate/stargate/issues/1200)
- Ability to batch-upload a document array with heterogeneous idPath [\#1191](https://github.com/stargate/stargate/issues/1191)
- Add support for merging AND queries [\#1184](https://github.com/stargate/stargate/issues/1184)
- Create expression rule for catching impossible filters to avoid any reads [\#1182](https://github.com/stargate/stargate/issues/1182)
- Simplify single value `$in` to `$eq` [\#1181](https://github.com/stargate/stargate/issues/1181)
- Add OpenRPC / JSON-RPC 2.0 Endpoints [\#1177](https://github.com/stargate/stargate/issues/1177)
- Documents API support for specifying additional partition / clustered keys [\#1169](https://github.com/stargate/stargate/issues/1169)
- Allow setting operators that cause ALLOW FILTERING as unsafe/disabled [\#1168](https://github.com/stargate/stargate/issues/1168)
- GraphQL schema-first: don't require authentication for custom directives reference [\#1167](https://github.com/stargate/stargate/issues/1167)
- Support expression simplification with predicate-specifc knowledge [\#1165](https://github.com/stargate/stargate/issues/1165)
- Add a DELETE call that uses a WHERE clause [\#1135](https://github.com/stargate/stargate/issues/1135)
- Explore moving from playground to graphiql [\#1129](https://github.com/stargate/stargate/issues/1129)
- Add comments to playground tabs [\#1128](https://github.com/stargate/stargate/issues/1128)
- GraphQL schema-first: add permission check for Federation's `\_service` query [\#1126](https://github.com/stargate/stargate/issues/1126)
- Return DocumentSearchResponseWrapper from searchDoc [\#1121](https://github.com/stargate/stargate/issues/1121)
- 404 error if send request to subdocument with "fields" but without "where". [\#1107](https://github.com/stargate/stargate/issues/1107)
- Count of the number of documents in the collection and subdocuments [\#1098](https://github.com/stargate/stargate/issues/1098)
- Spoof table `system.peer\_v2` [\#1097](https://github.com/stargate/stargate/issues/1097)
- Support native\_transport\_max\_negotiable\_protocol\_version in CQL service [\#1096](https://github.com/stargate/stargate/issues/1096)
- API Contract Testing [\#1081](https://github.com/stargate/stargate/issues/1081)
- Remove transitional base64 parameter decoding code [\#1044](https://github.com/stargate/stargate/issues/1044)
- Query executor should record execution context on the execute method [\#1025](https://github.com/stargate/stargate/issues/1025)
- Cache prepared statement in stargate [\#931](https://github.com/stargate/stargate/issues/931)
- Use AuthenticationService to control cache duration in Db.java [\#915](https://github.com/stargate/stargate/issues/915)
- Provide helper scripts / maven commands for building with local DSE jars [\#899](https://github.com/stargate/stargate/issues/899)
- Log warnings for default config not suitable for production use cases [\#801](https://github.com/stargate/stargate/issues/801)
- Add new options for selecting persistence module to load [\#780](https://github.com/stargate/stargate/issues/780)
- Add additional system properties to support other keyspace settings for tablebased token keyspace [\#739](https://github.com/stargate/stargate/issues/739)
- Expose the client metrics from `StorageProxy` in the persistence backends though Stargate metrics [\#637](https://github.com/stargate/stargate/issues/637)

**Merged pull requests:**

- Merging branch main into v2.1 [\#2817](https://github.com/stargate/stargate/pull/2817) ([github-actions[bot]](https://github.com/apps/github-actions))
- querybuilder and, or support  [\#2815](https://github.com/stargate/stargate/pull/2815) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Merging branch main into v2.1 [\#2814](https://github.com/stargate/stargate/pull/2814) ([github-actions[bot]](https://github.com/apps/github-actions))
- Bump aws-actions/amazon-ecr-login from 1 to 2 [\#2811](https://github.com/stargate/stargate/pull/2811) ([dependabot[bot]](https://github.com/apps/dependabot))
- use openjdk runtime images [\#2809](https://github.com/stargate/stargate/pull/2809) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bump version to 2.1.0-BETA-2-SNAPSHOT [\#2808](https://github.com/stargate/stargate/pull/2808) ([github-actions[bot]](https://github.com/apps/github-actions))

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

