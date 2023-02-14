# Changelog

## [v2.0.8](https://github.com/stargate/stargate/tree/v2.0.8) (2023-02-10)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.7...v2.0.8)

**Closed issues:**

- Update to DSE 6.8.32 [\#2428](https://github.com/stargate/stargate/issues/2428)

**Merged pull requests:**

- Fix postman-docker workflow [\#2427](https://github.com/stargate/stargate/pull/2427) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- disabling flaky StargateBridgeInterceptorDeadlineTest [\#2432](https://github.com/stargate/stargate/pull/2432) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Merge DSE-6.8.32 changes from v1 to main [\#2431](https://github.com/stargate/stargate/pull/2431) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- merge v1 workflow fix [\#2426](https://github.com/stargate/stargate/pull/2426) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- sign images with additional tags during the release [\#2424](https://github.com/stargate/stargate/pull/2424) ([ivansenic](https://github.com/ivansenic))
- trying to fix flaky deadline test [\#2422](https://github.com/stargate/stargate/pull/2422) ([ivansenic](https://github.com/ivansenic))
- Merge \#2418 from v1 - Add helper method for logging in Starter.java \(updated\) [\#2421](https://github.com/stargate/stargate/pull/2421) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#2417 for SGv2 separately: upgrade DropWizard version, deps [\#2420](https://github.com/stargate/stargate/pull/2420) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Externalizing Cassandra and Coordinator timeout values for test container [\#2415](https://github.com/stargate/stargate/pull/2415) ([maheshrajamani](https://github.com/maheshrajamani))
- Merge \#2405 fix from v1 to main [\#2411](https://github.com/stargate/stargate/pull/2411) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- closes \#2389: fix retriable stargate bridge being exposed as service [\#2410](https://github.com/stargate/stargate/pull/2410) ([ivansenic](https://github.com/ivansenic))
- update to Quarkus 2.16.0 [\#2409](https://github.com/stargate/stargate/pull/2409) ([ivansenic](https://github.com/ivansenic))
- Merge \#2144 fix from v1 to main [\#2407](https://github.com/stargate/stargate/pull/2407) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Try to fix some of 3 ITs in Stargate 2.0.6/2.0.7 that fail against CNDB backend [\#2403](https://github.com/stargate/stargate/pull/2403) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bump version to v2.0.8-SNAPSHOT, update release notes for v2.0.7 [\#2398](https://github.com/stargate/stargate/pull/2398) ([github-actions[bot]](https://github.com/apps/github-actions))
- Make inclusion of /v2/cql optional for REST API, with sysprop `stargate.rest.cql.disabled` \(use Quarkus 2.16 `@EndpointDisabled`\) [\#2319](https://github.com/stargate/stargate/pull/2319) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.0.7](https://github.com/stargate/stargate/tree/v2.0.7) (2023-01-23)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.6...v2.0.7)

**Closed issues:**

- Update DSE to 6.8.31 [\#2356](https://github.com/stargate/stargate/issues/2356)
- Add Stargate homepage to the V2 services [\#2177](https://github.com/stargate/stargate/issues/2177)

**Merged pull requests:**

- Update APIs to use same C\*/DSE versions as coordinator [\#2395](https://github.com/stargate/stargate/pull/2395) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- GH action cache cleanup on PR close [\#2394](https://github.com/stargate/stargate/pull/2394) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Create release artifacts for Stargate v2 APIs [\#2388](https://github.com/stargate/stargate/pull/2388) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix typo in README [\#2383](https://github.com/stargate/stargate/pull/2383) ([shashankbrgowda](https://github.com/shashankbrgowda))
- closes \#2376: reoarganize testing common utilities [\#2377](https://github.com/stargate/stargate/pull/2377) ([ivansenic](https://github.com/ivansenic))
- Use Optimistic Queries for REST API [\#2373](https://github.com/stargate/stargate/pull/2373) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.0.6](https://github.com/stargate/stargate/tree/v2.0.6) (2023-01-12)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.5...v2.0.6)

**Fixed bugs:**

- REST API tests fail with `Abrupt GOAWAY closed sent stream` [\#2209](https://github.com/stargate/stargate/issues/2209)

**Closed issues:**

- Mark REST v1 endpoints as Deprecated [\#2339](https://github.com/stargate/stargate/issues/2339)
- Mark Cassandra 3.11 Persistence as Deprecated [\#2254](https://github.com/stargate/stargate/issues/2254)
- Implement optimistic bridge queries in `sgv2-quarkus-common` [\#1977](https://github.com/stargate/stargate/issues/1977)

**Merged pull requests:**

- Fix \#2362: separate "keyspace" as query param vs older as path param [\#2372](https://github.com/stargate/stargate/pull/2372) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update quarkus to 2.15.3 [\#2371](https://github.com/stargate/stargate/pull/2371) ([ivansenic](https://github.com/ivansenic))
- Implement retriable bridge calls [\#2363](https://github.com/stargate/stargate/pull/2363) ([ivansenic](https://github.com/ivansenic))
- Add support for optimistic queries in the common [\#2355](https://github.com/stargate/stargate/pull/2355) ([ivansenic](https://github.com/ivansenic))

## [v2.0.5](https://github.com/stargate/stargate/tree/v2.0.5) (2023-01-10)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.4...v2.0.5)

**Fixed bugs:**

- GraphQL V2 not handling `JsonParseException` [\#2314](https://github.com/stargate/stargate/issues/2314)
- Keyspace Creation with DC information KO in V2 [\#2231](https://github.com/stargate/stargate/issues/2231)

**Closed issues:**

- SGv2 REST API should NOT do `authorizeSchemaRead` for regular DML CRUD operations [\#2349](https://github.com/stargate/stargate/issues/2349)
- Update Jackson dependency from 2.13.4 to 2.14.1 to solve CVEs [\#2345](https://github.com/stargate/stargate/issues/2345)
- REST tests should not create a keyspace for each test method [\#2326](https://github.com/stargate/stargate/issues/2326)
- Mark REST v1 endpoints as Deprecated [\#2339](https://github.com/stargate/stargate/issues/2339)
- Delete keyspaces after each integration test [\#2321](https://github.com/stargate/stargate/issues/2321)
- Update Cassandra-4.0 backend to 4.0.7 \(from 4.0.4\) [\#2261](https://github.com/stargate/stargate/issues/2261)
- Mark Cassandra 3.11 Persistence as Deprecated [\#2254](https://github.com/stargate/stargate/issues/2254)
- Move ExceptionMappers out of sgv2-quarkus-common [\#2248](https://github.com/stargate/stargate/issues/2248)
- Remove source api from schema reads [\#2195](https://github.com/stargate/stargate/issues/2195)
- Add deadline for the client side gRPC in the V2 [\#2192](https://github.com/stargate/stargate/issues/2192)
- Separate CI images [\#1768](https://github.com/stargate/stargate/issues/1768)
- Configure Stargate v2 Docker images to run as non-root [\#1707](https://github.com/stargate/stargate/issues/1707)

**Merged pull requests:**

- add client deadlines to the bridge interceptor [\#2352](https://github.com/stargate/stargate/pull/2352) ([ivansenic](https://github.com/ivansenic))
- Avoid SchemaReadCheck optional for Row CRUD \(leave for Schema access\) [\#2351](https://github.com/stargate/stargate/pull/2351) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2345: Upgrade Jackson dep to 2.14.1 \(from 2.13.4\) [\#2350](https://github.com/stargate/stargate/pull/2350) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2339: mark RESTv1 API as deprecated \(as well as in-coordinator APIs\) [\#2347](https://github.com/stargate/stargate/pull/2347) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2326: reduce amount of keyspaces created by ITs, add clean up [\#2341](https://github.com/stargate/stargate/pull/2341) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update v2 coordinator docker images to run as non-root user [\#2340](https://github.com/stargate/stargate/pull/2340) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- add parameterized query to gRPC Java example [\#2338](https://github.com/stargate/stargate/pull/2338) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- add workflow to test postman collections against docker compose scripts [\#2337](https://github.com/stargate/stargate/pull/2337) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- organize shared production and dev props [\#2335](https://github.com/stargate/stargate/pull/2335) ([ivansenic](https://github.com/ivansenic))
- document version support and branching strategy on v2 [\#2334](https://github.com/stargate/stargate/pull/2334) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- ux improvements to docker compose configs [\#2333](https://github.com/stargate/stargate/pull/2333) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- relates to \#2195: remove api source from schema read [\#2330](https://github.com/stargate/stargate/pull/2330) ([ivansenic](https://github.com/ivansenic))
- remove async reponse from graphql [\#2328](https://github.com/stargate/stargate/pull/2328) ([ivansenic](https://github.com/ivansenic))
- closes \#2314: added test to test gql invalid json response [\#2327](https://github.com/stargate/stargate/pull/2327) ([ivansenic](https://github.com/ivansenic))
- fix typo in config prop name [\#2325](https://github.com/stargate/stargate/pull/2325) ([ivansenic](https://github.com/ivansenic))
- closes \#2321: delete keyspaces on the integration test end [\#2324](https://github.com/stargate/stargate/pull/2324) ([ivansenic](https://github.com/ivansenic))

## [v2.0.4](https://github.com/stargate/stargate/tree/v2.0.4) (2022-12-22)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.3...v2.0.4)

**Closed issues:**

- Move ExceptionMappers out of sgv2-quarkus-common [\#2248](https://github.com/stargate/stargate/issues/2248)
- Add deadline for the client side gRPC in the V2 [\#2192](https://github.com/stargate/stargate/issues/2192)
- The bridge gRPC status `DEADLINE\_EXCEEDED` in the V2 APIs should resolve to `HTTP 504` [\#2318](https://github.com/stargate/stargate/issues/2318)
- Add new path to REST API for CQL entrypoint \(/v2/cql\) [\#2313](https://github.com/stargate/stargate/issues/2313)
- Update to Quarkus `v2.15.0` fails due to the missing class `graphql.parser.antlr.GraphqlLexer` [\#2309](https://github.com/stargate/stargate/issues/2309)
- Change SGv2/REST DTOs to be simple Record types where possible [\#2264](https://github.com/stargate/stargate/issues/2264)

**Merged pull requests:**

- closes \#2318: grpc deadline exceeded to be reported as HTTP 504 [\#2320](https://github.com/stargate/stargate/pull/2320) ([ivansenic](https://github.com/ivansenic))
- closes \#2248: configurable exception handling and mapping [\#2317](https://github.com/stargate/stargate/pull/2317) ([ivansenic](https://github.com/ivansenic))
- no default int test profile in the sgv2 apis [\#2315](https://github.com/stargate/stargate/pull/2315) ([ivansenic](https://github.com/ivansenic))
- closes \#2309: update quarkus to v2.15.1 [\#2308](https://github.com/stargate/stargate/pull/2308) ([ivansenic](https://github.com/ivansenic))
- Convert REST Schema/ DTOs to Records, improve Swagger descs [\#2305](https://github.com/stargate/stargate/pull/2305) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Ingress resource name fixed for docs and graphql API [\#2303](https://github.com/stargate/stargate/pull/2303) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#2231: map exception for keyspace creation failure to more meaningful [\#2302](https://github.com/stargate/stargate/pull/2302) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- updates quarkus to v2.14.3 [\#2301](https://github.com/stargate/stargate/pull/2301) ([ivansenic](https://github.com/ivansenic))
- fixes  openapi example in docs v2 [\#2300](https://github.com/stargate/stargate/pull/2300) ([ivansenic](https://github.com/ivansenic))
- Bump version to `2.0.4-SNAPSHOT` [\#2299](https://github.com/stargate/stargate/pull/2299) ([github-actions[bot]](https://github.com/apps/github-actions))
- Allow CORS to REST api for dev [\#2284](https://github.com/stargate/stargate/pull/2284) ([tjake](https://github.com/tjake))
- Add CQL endpoint to restapi such that webapps can execute cql statements directly [\#2266](https://github.com/stargate/stargate/pull/2266) ([tjake](https://github.com/tjake))

**Merged pull requests:**

## [v2.0.3](https://github.com/stargate/stargate/tree/v2.0.3) (2022-12-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.2...v2.0.3)

**Merged pull requests:**

- Merge revert \#2296 of early-auth-check \(\#619\) into main \(v2\) [\#2297](https://github.com/stargate/stargate/pull/2297) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix release v2 workflow failing due to the branding issues [\#2295](https://github.com/stargate/stargate/pull/2295) ([ivansenic](https://github.com/ivansenic))

## [v2.0.2](https://github.com/stargate/stargate/tree/v2.0.2) (2022-12-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.1...v2.0.2)

**Fixed bugs:**

- DSE dependencies broken by the old `dse-java-driver-core` [\#2269](https://github.com/stargate/stargate/issues/2269)

**Closed issues:**

- Inject build version number of APIs into `index.html` \(or another easily accessible place\) [\#2285](https://github.com/stargate/stargate/issues/2285)
- Add `min-chars` and `regex` to subdomain tenant resolver [\#2280](https://github.com/stargate/stargate/issues/2280)
- We should check the result of this uni \(check all the booleans in the list\) in `Sgv2IndexesResourceImpl` [\#2268](https://github.com/stargate/stargate/issues/2268)
- Replace SGv2/REST explicit input parameter validation with Java Bean Validation annotations [\#2263](https://github.com/stargate/stargate/issues/2263)
- Improve SGv2/REST-API async endpoint signatures to be typed [\#2262](https://github.com/stargate/stargate/issues/2262)
- Index Creation KO: mark existing column as not present. [\#2244](https://github.com/stargate/stargate/issues/2244)
- Poor document read performance in Docs API V2 [\#1996](https://github.com/stargate/stargate/issues/1996)

**Merged pull requests:**

- Complete \#2285 by adding version info to docs, graphql too & refactor [\#2289](https://github.com/stargate/stargate/pull/2289) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- updating docker building script, update release workflow [\#2287](https://github.com/stargate/stargate/pull/2287) ([ivansenic](https://github.com/ivansenic))
- Add simple substitution for Maven project.version into "index.html" [\#2286](https://github.com/stargate/stargate/pull/2286) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#2280: regex validation in the subdomain tenant resolver [\#2283](https://github.com/stargate/stargate/pull/2283) ([ivansenic](https://github.com/ivansenic))
- Fix \#2268: check access rights as expected \(plus IT improvements\) [\#2279](https://github.com/stargate/stargate/pull/2279) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2262: use typed responses for REST/Async endpoints where possible [\#2278](https://github.com/stargate/stargate/pull/2278) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- remove page size default value from swagger for get doc [\#2277](https://github.com/stargate/stargate/pull/2277) ([ivansenic](https://github.com/ivansenic))
- Convert REST API to use Validation API annotations instead of explicit checks [\#2276](https://github.com/stargate/stargate/pull/2276) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2244: include all columns in existence check wrt index creation [\#2275](https://github.com/stargate/stargate/pull/2275) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- setup the branding for the ECR images [\#2271](https://github.com/stargate/stargate/pull/2271) ([ivansenic](https://github.com/ivansenic))
- Update Netty version to align with one DSE uses [\#2260](https://github.com/stargate/stargate/pull/2260) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- upgrade DSE to v6.8.29 in the V2 APIs [\#2258](https://github.com/stargate/stargate/pull/2258) ([versaurabh](https://github.com/versaurabh))
- Add explicit handling of "empty" Tuples Bridge returns for missing Tuples [\#2256](https://github.com/stargate/stargate/pull/2256) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add prefix to Cassandra/Coordinator container log redirects [\#2255](https://github.com/stargate/stargate/pull/2255) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2245: allow numeric timestamp for CQL `timestamp` valued columns [\#2252](https://github.com/stargate/stargate/pull/2252) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update Quarkus to `v2.14.1.Final` [\#2249](https://github.com/stargate/stargate/pull/2249) ([ivansenic](https://github.com/ivansenic))
- document exposed endpoints from docker compose scripts [\#2243](https://github.com/stargate/stargate/pull/2243) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Allow more Timezone formats for "Instant" in REST API [\#2241](https://github.com/stargate/stargate/pull/2241) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Convert SGv2/REST to Async handling with Quarkus/Mutiny [\#2213](https://github.com/stargate/stargate/pull/2213) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.0.1](https://github.com/stargate/stargate/tree/v2.0.1) (2022-11-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0...v2.0.1)

**Merged pull requests:**

- Make InboundHAProxyHandler @Sharable [\#2233](https://github.com/stargate/stargate/pull/2233) ([jakubzytka](https://github.com/jakubzytka))
- Bump graphql-java from 18.1 to 18.3 in /graphqlapi on `v1` [\#2230](https://github.com/stargate/stargate/pull/2230) ([ivansenic](https://github.com/ivansenic))
- organize shared dependencies in common project [\#2227](https://github.com/stargate/stargate/pull/2227) ([ivansenic](https://github.com/ivansenic))
- use getIfPresent in SchemaManager [\#2226](https://github.com/stargate/stargate/pull/2226) ([ivansenic](https://github.com/ivansenic))
- Fix for \#2221 -\> enhanced logging for the gRPC bridge exceptions [\#2225](https://github.com/stargate/stargate/pull/2225) ([versaurabh](https://github.com/versaurabh))
- Bump graphql-java from 18.1 to 18.3 in /apis/sgv2-graphqlapi [\#2222](https://github.com/stargate/stargate/pull/2222) ([dependabot[bot]](https://github.com/apps/dependabot))
- relates to \#2177: basic branding of the index pages [\#2219](https://github.com/stargate/stargate/pull/2219) ([ivansenic](https://github.com/ivansenic))
- closes \#2201: playground to auto-inject token from headers [\#2217](https://github.com/stargate/stargate/pull/2217) ([ivansenic](https://github.com/ivansenic))
- closes \#2159: moved common configuration to sgv2-quarkus-commons  [\#2216](https://github.com/stargate/stargate/pull/2216) ([ivansenic](https://github.com/ivansenic))
- update Quarkus to v2.13.4 [\#2215](https://github.com/stargate/stargate/pull/2215) ([ivansenic](https://github.com/ivansenic))
- closes \#2209: add transient retries to the grpc client [\#2214](https://github.com/stargate/stargate/pull/2214) ([ivansenic](https://github.com/ivansenic))
- Fix \#2200: include auth header for OpenAPI \(Swagger\) by adding annota… [\#2202](https://github.com/stargate/stargate/pull/2202) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update changelog and bump version to `v2.0.1-SNAPSHOT` [\#2196](https://github.com/stargate/stargate/pull/2196) ([ivansenic](https://github.com/ivansenic))
- Bump graphql-java from 18.1 to 18.3 in /graphqlapi [\#2080](https://github.com/stargate/stargate/pull/2080) ([dependabot[bot]](https://github.com/apps/dependabot))

## [v2.0.0](https://github.com/stargate/stargate/tree/v2.0.0) (2022-10-25)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-RC-1...v2.0.0)

**Fixed bugs:**

- Bridge service is not handling runtime exceptions [\#2179](https://github.com/stargate/stargate/issues/2179)
- API V2 bridge communication has wrong Source API [\#2175](https://github.com/stargate/stargate/issues/2175)
- Bridge service uses the `Context.Key` without context propagation [\#2181](https://github.com/stargate/stargate/issues/2181)

**Closed issues:**

- Separate openapi paths for Docs and REST V2 [\#2182](https://github.com/stargate/stargate/issues/2182)
- Enabling Quarkus access log must not print the access token [\#2176](https://github.com/stargate/stargate/issues/2176)

**Merged pull requests:**

- Second part of fix to \#2179: handle "executeQueryWithSchema\(\)" too [\#2194](https://github.com/stargate/stargate/pull/2194) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- enforce JDK 17+ for building SGv2 APIs [\#2193](https://github.com/stargate/stargate/pull/2193) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update version to v2.0.0-SNAPSHOT [\#2191](https://github.com/stargate/stargate/pull/2191) ([ivansenic](https://github.com/ivansenic))
- closes \#2175: apis to report source api in the grpc metadata [\#2190](https://github.com/stargate/stargate/pull/2190) ([ivansenic](https://github.com/ivansenic))
- Update DropWizard 2.0.32-\>2.0.34 to get commons-text upgraded [\#2188](https://github.com/stargate/stargate/pull/2188) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix handling of `describeKeyspace` to catch RuntimeExceptions too, add a test [\#2187](https://github.com/stargate/stargate/pull/2187) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- OpenAPI schema URL context path changes [\#2186](https://github.com/stargate/stargate/pull/2186) ([kathirsvn](https://github.com/kathirsvn))
- closes \#2181: bridge context key not to be used in another thread [\#2184](https://github.com/stargate/stargate/pull/2184) ([ivansenic](https://github.com/ivansenic))
- enforce JDK 8 [\#2183](https://github.com/stargate/stargate/pull/2183) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix to skip printing the token received in the request header in the … [\#2178](https://github.com/stargate/stargate/pull/2178) ([kathirsvn](https://github.com/kathirsvn))

## [v2.0.0-RC-1](https://github.com/stargate/stargate/tree/v2.0.0-RC-1) (2022-10-18)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-BETA-5...v2.0.0-RC-1)

**Closed issues:**

- Cosign images for V2 not working [\#2140](https://github.com/stargate/stargate/issues/2140)

**Merged pull requests:**

- Bumping version for the v2 release candidate [\#2173](https://github.com/stargate/stargate/pull/2173) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v2.0.0-BETA-5](https://github.com/stargate/stargate/tree/v2.0.0-BETA-5) (2022-10-18)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-BETA-4...v2.0.0-BETA-5)

**Fixed bugs:**

- Project name, description and url missing in the API V2 parent pom.xml [\#2145](https://github.com/stargate/stargate/issues/2145)
- Swagger in Docs API v2 creates a wrong curl command [\#2131](https://github.com/stargate/stargate/issues/2131)

**Closed issues:**

- Add option to limit subdomain tenant resolver to a part of the subdomain [\#2164](https://github.com/stargate/stargate/issues/2164)
- Can not re-run failing V2 API tests due to eager \("always"\) Docker cleanup [\#2163](https://github.com/stargate/stargate/issues/2163)
- Remove unneeded `StargateTestResource` option `DISABLE\_FIXED\_TOKEN` \(default to `true`\) [\#2151](https://github.com/stargate/stargate/issues/2151)
- The `set-output` command deprecated in GitHub actions [\#2150](https://github.com/stargate/stargate/issues/2150)
- Docker-compose scripts should allow enabling request log by Quarkus [\#2148](https://github.com/stargate/stargate/issues/2148)
- Docker-compose scripts should allow changing default log level \(from INFO to DEBUG\) [\#2141](https://github.com/stargate/stargate/issues/2141)
- REST API build complains about the wrong property name [\#2135](https://github.com/stargate/stargate/issues/2135)
- Build fails on M1 macs due to unavailable binaries [\#2073](https://github.com/stargate/stargate/issues/2073)

**Merged pull requests:**

- Add logging of gRPC failures in case where problem is from server-sid… [\#2171](https://github.com/stargate/stargate/pull/2171) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update Quarkus to `2.13.2` [\#2170](https://github.com/stargate/stargate/pull/2170) ([ivansenic](https://github.com/ivansenic))
- relates to \#2150: removes set-output from the V2 GH workflows [\#2168](https://github.com/stargate/stargate/pull/2168) ([ivansenic](https://github.com/ivansenic))
- closes \#2163: don't delete docker imgs in failed GH workflows [\#2167](https://github.com/stargate/stargate/pull/2167) ([ivansenic](https://github.com/ivansenic))
- closes \#2164: add option to limit max chars in the sub-domain for ten… [\#2165](https://github.com/stargate/stargate/pull/2165) ([ivansenic](https://github.com/ivansenic))
- Remove intermediate DropWizard-based SGv2/REST API [\#2162](https://github.com/stargate/stargate/pull/2162) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add `-q` option to docker-compose scripts to enable Quarkus request logging [\#2161](https://github.com/stargate/stargate/pull/2161) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Quarkus log format is fixed to match the format defined in logback.xm… [\#2158](https://github.com/stargate/stargate/pull/2158) ([kathirsvn](https://github.com/kathirsvn))
- closes \#2131: fixes swagger issues in the docs api v2 [\#2157](https://github.com/stargate/stargate/pull/2157) ([ivansenic](https://github.com/ivansenic))
- fix RestApiV2QCqlEnabledTestBase to use C\* auth if needed [\#2153](https://github.com/stargate/stargate/pull/2153) ([ivansenic](https://github.com/ivansenic))
- Remove `StargateTestResource.Options.DISABLE\_FIXED\_TOKEN`, all usage [\#2152](https://github.com/stargate/stargate/pull/2152) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Convert DSE-backed REST API tests SG v1-\>v2 [\#2149](https://github.com/stargate/stargate/pull/2149) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2132 by adding missing properties to parent pom [\#2146](https://github.com/stargate/stargate/pull/2146) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add `-r` option for docker-compose scripts for configuring default log level [\#2143](https://github.com/stargate/stargate/pull/2143) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Stargate v2 Docker usability [\#2139](https://github.com/stargate/stargate/pull/2139) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- disable vert.x uri validation in the v2 docker images [\#2138](https://github.com/stargate/stargate/pull/2138) ([ivansenic](https://github.com/ivansenic))
- Bumping version to 2.0.0-BETA-5-SNAPSHOT [\#2137](https://github.com/stargate/stargate/pull/2137) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v2.0.0-BETA-4](https://github.com/stargate/stargate/tree/v2.0.0-BETA-4) (2022-10-04)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-BETA-3...v2.0.0-BETA-4)

**Closed issues:**

- Use `-ntp` \(no-transfer-progress\) option for Github action maven invocations [\#2124](https://github.com/stargate/stargate/issues/2124)
- Define Quarkus micrometer "match-patterns" setting for REST API [\#2116](https://github.com/stargate/stargate/issues/2116)

**Merged pull requests:**

- Fix \#2135 add open telemetry to rest, graphql apis [\#2136](https://github.com/stargate/stargate/pull/2136) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- added health extension to the graphql v2 [\#2133](https://github.com/stargate/stargate/pull/2133) ([ivansenic](https://github.com/ivansenic))
- publish to OSSRH not to fail job on error [\#2132](https://github.com/stargate/stargate/pull/2132) ([ivansenic](https://github.com/ivansenic))
- Add "match-patterns" for REST API [\#2130](https://github.com/stargate/stargate/pull/2130) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Try to recreate fix from PR \#2074, to fix Mac M1 proto build [\#2128](https://github.com/stargate/stargate/pull/2128) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix DSE version upgrade changes that were not merged due to version c… [\#2127](https://github.com/stargate/stargate/pull/2127) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2124: add `-ntp` flag for CI Maven invocations to avoid printing progress msgs [\#2125](https://github.com/stargate/stargate/pull/2125) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
-  Allow executing a built in function against the root of a document  [\#2067](https://github.com/stargate/stargate/pull/2067) ([EricBorczuk](https://github.com/EricBorczuk))

## [v2.0.0-BETA-3](https://github.com/stargate/stargate/tree/v2.0.0-BETA-3) (2022-09-30)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-BETA-2...v2.0.0-BETA-3)

**Merged pull requests:**

- relates to \#2108: fix bridge interceptor context check [\#2115](https://github.com/stargate/stargate/pull/2115) ([ivansenic](https://github.com/ivansenic))
- align base configuration for the quarkus based apis [\#2114](https://github.com/stargate/stargate/pull/2114) ([ivansenic](https://github.com/ivansenic))
- Switch port numbers for in-coordinator/extracted-drop-wizard cases [\#2113](https://github.com/stargate/stargate/pull/2113) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- remove duplicate port usage from docker compose files [\#2112](https://github.com/stargate/stargate/pull/2112) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Remove old Documents API integration tests; replaced by ones in `apis/sgv2-docsapi` [\#2109](https://github.com/stargate/stargate/pull/2109) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix docker compose ports [\#2105](https://github.com/stargate/stargate/pull/2105) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update quarkus version to 2.12.3 [\#2102](https://github.com/stargate/stargate/pull/2102) ([ivansenic](https://github.com/ivansenic))
- eliminating obsolete extra starctl scripts [\#2093](https://github.com/stargate/stargate/pull/2093) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- fix flaky graphql v2 int tests [\#2092](https://github.com/stargate/stargate/pull/2092) ([ivansenic](https://github.com/ivansenic))
- Fix "all keyspaces" test to work with CNDB backend [\#2088](https://github.com/stargate/stargate/pull/2088) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add retry for data store properties access; new property to enable use of fallbacks [\#2087](https://github.com/stargate/stargate/pull/2087) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- option to enable auth in the int tests [\#2086](https://github.com/stargate/stargate/pull/2086) ([ivansenic](https://github.com/ivansenic))
- updating project description [\#2097](https://github.com/stargate/stargate/pull/2097) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- relates to \#2065: bump DSE version to 6.8.26  [\#2094](https://github.com/stargate/stargate/pull/2094) ([ivansenic](https://github.com/ivansenic))

## [v2.0.0-BETA-2](https://github.com/stargate/stargate/tree/v2.0.0-BETA-2) (2022-09-17)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-BETA-1...v2.0.0-BETA-2)

**Closed issues:**

- Improve `docker-compose` scripts to wait for Coordinator to start up before APIs [\#2076](https://github.com/stargate/stargate/issues/2076)
- Update SnakeYAML dependency to 1.32 to resolve CVE-2022-38725 [\#2078](https://github.com/stargate/stargate/issues/2078)
- Fix \#2061, problem constructing converter for deeply nested structured type

**Merged pull requests:**

- Fix docker-compose startup sequence \(\#2076\)  [\#2077](https://github.com/stargate/stargate/pull/2077) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change sgv2 REST API default port back to 8082 [\#2072](https://github.com/stargate/stargate/pull/2072) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add Docs API to compose scripts [\#2071](https://github.com/stargate/stargate/pull/2071) ([EricBorczuk](https://github.com/EricBorczuk))
- Unify REST sgv2 \(Q\) configuration to be more similar to Docs API; increase Coordinator container startup timeout [\#2069](https://github.com/stargate/stargate/pull/2069) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update DEV\_GUIDE.md [\#2064](https://github.com/stargate/stargate/pull/2064) ([EricBorczuk](https://github.com/EricBorczuk))
- Fix \#2061, problem constructing converter for deeply nested structured type [\#2063](https://github.com/stargate/stargate/pull/2063) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v2.0.0-BETA-1](https://github.com/stargate/stargate/tree/v2.0.0-BETA-1) (2022-09-08)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-17...v2.0.0-BETA-1)

**Merged pull requests:**

- Stargate v2 readme updates [\#2055](https://github.com/stargate/stargate/pull/2055) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Remove unneeded tests from SGv2 \(RESTv1 not supported by the new backend\) [\#2051](https://github.com/stargate/stargate/pull/2051) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- enable population of the token in the swagger-ui [\#2050](https://github.com/stargate/stargate/pull/2050) ([ivansenic](https://github.com/ivansenic))
- closes \#2031: dead leaves to accept custom metadata [\#2048](https://github.com/stargate/stargate/pull/2048) ([ivansenic](https://github.com/ivansenic))
- update int tests readme [\#2043](https://github.com/stargate/stargate/pull/2043) ([ivansenic](https://github.com/ivansenic))
- $set support [\#2040](https://github.com/stargate/stargate/pull/2040) ([EricBorczuk](https://github.com/EricBorczuk))
- repository should not include stargateio [\#2039](https://github.com/stargate/stargate/pull/2039) ([ivansenic](https://github.com/ivansenic))
- making all int test in Docs V2 to use @QuarkusIntegrationTest [\#2029](https://github.com/stargate/stargate/pull/2029) ([ivansenic](https://github.com/ivansenic))
- Separate executor in BridgeImpl [\#2019](https://github.com/stargate/stargate/pull/2019) ([EricBorczuk](https://github.com/EricBorczuk))
- Convert SGv2 REST API from DropWizard to Quarkus [\#1982](https://github.com/stargate/stargate/pull/1982) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Convert GraphQL v2 to Quarkus [\#1980](https://github.com/stargate/stargate/pull/1980) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-17](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-17) (2022-08-16)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-16...v2.0.0-ALPHA-17)

**Fixed bugs:**

- Document API V2 converts infrastructure exceptions to `5xx` [\#1928](https://github.com/stargate/stargate/issues/1928)

**Closed issues:**

- Avoid creating a new grpc stub for each request in the `StargateRequestInfo` [\#2004](https://github.com/stargate/stargate/issues/2004)
- `StargateV1ConfigurationSourceProviderTest` failing on v2 branch [\#2001](https://github.com/stargate/stargate/issues/2001)
- Move all generic and shared components to the `svg2-quarkus-common` [\#1983](https://github.com/stargate/stargate/issues/1983)
- Consider alternative setup for sgv2-quarkus-common test JAR [\#1976](https://github.com/stargate/stargate/issues/1976)
- Enhanced querying is not needed always in the `QueryExecutor` [\#1960](https://github.com/stargate/stargate/issues/1960)
- Final setup for the Docs API V2 [\#1823](https://github.com/stargate/stargate/issues/1823)
- Use comparable bytes API in persistence-cassandra-3.11 and persistence-cassandra-4.0 [\#1761](https://github.com/stargate/stargate/issues/1761)
- Independent end-to-end tests in Document API V2 [\#1737](https://github.com/stargate/stargate/issues/1737)
- Support document table upgrade functions in Document API V2 [\#1736](https://github.com/stargate/stargate/issues/1736)
- Support document built-in functions in the Document API V2 [\#1735](https://github.com/stargate/stargate/issues/1735)
- REST Controllers for the read and search paths in the Document API V2 [\#1734](https://github.com/stargate/stargate/issues/1734)
- REST Controllers for the write paths in the Document API V2 [\#1730](https://github.com/stargate/stargate/issues/1730)

**Merged pull requests:**

- Add one more IT for RESTv2/DELETE to cover missing/invalid item refs [\#2036](https://github.com/stargate/stargate/pull/2036) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#2034: update okhttp test dependency [\#2035](https://github.com/stargate/stargate/pull/2035) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next release [\#2033](https://github.com/stargate/stargate/pull/2033) ([github-actions[bot]](https://github.com/apps/github-actions))
- using skipPublish for the build without publish as well [\#2032](https://github.com/stargate/stargate/pull/2032) ([ivansenic](https://github.com/ivansenic))
- upgrade Quarkus to 2.11.2, DSE to 6.8.25 [\#2030](https://github.com/stargate/stargate/pull/2030) ([ivansenic](https://github.com/ivansenic))
- Fix merge issue [\#2027](https://github.com/stargate/stargate/pull/2027) ([olim7t](https://github.com/olim7t))
- V2 fix: id-path now accepts a path to a non-string value [\#2021](https://github.com/stargate/stargate/pull/2021) ([EricBorczuk](https://github.com/EricBorczuk))
- Update README with notes on troubelshooting IT run failure [\#2017](https://github.com/stargate/stargate/pull/2017) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- SGv2: Deterministic hash codes for schema objects [\#2010](https://github.com/stargate/stargate/pull/2010) ([mpenick](https://github.com/mpenick))
- Avoid creating a new grpc stub for each request in the StargateRequestInfo [\#2008](https://github.com/stargate/stargate/pull/2008) ([EricBorczuk](https://github.com/EricBorczuk))
- fix bugs in application props and configuration readme [\#1998](https://github.com/stargate/stargate/pull/1998) ([ivansenic](https://github.com/ivansenic))
- Move integration test utilities to sgv2-quarkus-common [\#1995](https://github.com/stargate/stargate/pull/1995) ([olim7t](https://github.com/olim7t))
- V1/V2 Docs API compat test [\#1990](https://github.com/stargate/stargate/pull/1990) ([EricBorczuk](https://github.com/EricBorczuk))
- Upgrade Quarkus 2.10.1-final -\> 2.10.3-final [\#1989](https://github.com/stargate/stargate/pull/1989) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- updated release workflow for V2 [\#1987](https://github.com/stargate/stargate/pull/1987) ([ivansenic](https://github.com/ivansenic))
- closes \#1823: fixes all warnings in docs api V2 [\#1986](https://github.com/stargate/stargate/pull/1986) ([ivansenic](https://github.com/ivansenic))
- closes \#1983: moving other common components to common project [\#1984](https://github.com/stargate/stargate/pull/1984) ([ivansenic](https://github.com/ivansenic))
- relates to \#1823: final setup for the docs api v2 [\#1970](https://github.com/stargate/stargate/pull/1970) ([ivansenic](https://github.com/ivansenic))
- closes \#1903: decrease fetch of the comparable bytes [\#1966](https://github.com/stargate/stargate/pull/1966) ([ivansenic](https://github.com/ivansenic))
- Extract common module for Quarkus-based services [\#1965](https://github.com/stargate/stargate/pull/1965) ([olim7t](https://github.com/olim7t))
- closes \#1735: execute builtin functions functionality [\#1964](https://github.com/stargate/stargate/pull/1964) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1761: copy and implement needed comprable bytes API for D… [\#1959](https://github.com/stargate/stargate/pull/1959) ([ivansenic](https://github.com/ivansenic))
- closes \#1736: upgrade actions for collections [\#1958](https://github.com/stargate/stargate/pull/1958) ([ivansenic](https://github.com/ivansenic))
- closes \#1737: update Quarkus to 2.10.1.Final, finalize int tests [\#1957](https://github.com/stargate/stargate/pull/1957) ([ivansenic](https://github.com/ivansenic))
- closes \#1928: web app exceptions to be intercepted [\#1952](https://github.com/stargate/stargate/pull/1952) ([ivansenic](https://github.com/ivansenic))
- relates to \#1730: document update resource tests [\#1951](https://github.com/stargate/stargate/pull/1951) ([ivansenic](https://github.com/ivansenic))
- relates to \#1730: document patch resource tests [\#1950](https://github.com/stargate/stargate/pull/1950) ([EricBorczuk](https://github.com/EricBorczuk))
- Bumping version to 2.0.0-ALPHA-17-SNAPSHOT [\#1949](https://github.com/stargate/stargate/pull/1949) ([github-actions[bot]](https://github.com/apps/github-actions))
- relates to \#1730: document write resource tests [\#1942](https://github.com/stargate/stargate/pull/1942) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1734: int tests for search, get document and sub-document [\#1930](https://github.com/stargate/stargate/pull/1930) ([ivansenic](https://github.com/ivansenic))

## [v2.0.0-ALPHA-16](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-16) (2022-07-05)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-15...v2.0.0-ALPHA-16)

**Merged pull requests:**

- update docs api V2 target C\* and DSE versions [\#1945](https://github.com/stargate/stargate/pull/1945)
- relates to \#1920: update C\* and DSE versions in the docker compose [\#1944](https://github.com/stargate/stargate/pull/1944)
- relates to \#1730: added document delete resource test [\#1933](https://github.com/stargate/stargate/pull/1933)
- relates to \#1737: docs api V2 action workflow updates [\#1931](https://github.com/stargate/stargate/pull/1931)

## [v2.0.0-ALPHA-15](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-15) (2022-07-01)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-14...v2.0.0-ALPHA-15)

**Merged pull requests:**

- Add JVM metrics to SGv2 REST API [\#1929](https://github.com/stargate/stargate/pull/1929)
- relates to \#1730, \#1734: api documents for write paths updated [\#1927](https://github.com/stargate/stargate/pull/1927)
- Fix \#1923: expose Table "default TTL" for SGv2/REST API too [\#1924](https://github.com/stargate/stargate/pull/1924)
- relates to \#1734: read resource [\#1921](https://github.com/stargate/stargate/pull/1921)
- Port write endpoints to v2 [\#1916](https://github.com/stargate/stargate/pull/1916)
- Push images to ECR [\#1911](https://github.com/stargate/stargate/pull/1911)
- updated Quarkus to v2.10.0.Final [\#1909](https://github.com/stargate/stargate/pull/1909)
- closes \#1887: fixed consistency for reads, added consistency checks in tests [\#1908](https://github.com/stargate/stargate/pull/1908)
- closes \#1732: read documents service for V2 [\#1907](https://github.com/stargate/stargate/pull/1907)
- closes \#1733: added dead leaves deletion to the write bridge service [\#1905](https://github.com/stargate/stargate/pull/1905)
- closes \#1729: port write paths of ReactiveDocumentService v1 to v2 [\#1899](https://github.com/stargate/stargate/pull/1899)
- DEV\_GUIDE: Add MacOS loopback address [\#1812](https://github.com/stargate/stargate/pull/1812)

## [v2.0.0-ALPHA-14](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-14) (2022-06-21)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-13...v2.0.0-ALPHA-14)

**Closed issues:**

- Implement `ReadBridgeService` in the Document API V2 [\#1738](https://github.com/stargate/stargate/issues/1738)
- Consider optimistic handling of schema metadata [\#1873](https://github.com/stargate/stargate/issues/1873)

**Merged pull requests:**

- closes \#1738: read bridge service for docs V2 [\#1888](https://github.com/stargate/stargate/pull/1888) ([ivansenic](https://github.com/ivansenic))
- Add optimistic handling of schema metadata \(fixes \#1873\) [\#1876](https://github.com/stargate/stargate/pull/1876) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-13](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-13) (2022-06-15)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-12...v2.0.0-ALPHA-13)

**Merged pull requests:**

- Revert the nesting of JSON schema in table comment [\#1885](https://github.com/stargate/stargate/pull/1885) ([EricBorczuk](https://github.com/EricBorczuk))
- Adds JsonSchemaResource, with tests [\#1867](https://github.com/stargate/stargate/pull/1867) ([EricBorczuk](https://github.com/EricBorczuk))

## [v2.0.0-ALPHA-12](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-12) (2022-06-10)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-11...v2.0.0-ALPHA-12)

**Fixed bugs:**

**Closed issues:**

- Support different resume modes for the paging state for enriched queries [\#1862](https://github.com/stargate/stargate/issues/1862)
- Stargate V2 keyspace creation via REST API does not work with `datacenters` argument [\#1817](https://github.com/stargate/stargate/issues/1817)
- Decouple Bridge from gRPC service [\#1770](https://github.com/stargate/stargate/issues/1770)
- Namespace REST Controller for Document API V2 [\#1724](https://github.com/stargate/stargate/issues/1724)

**Merged pull requests:**

- Fix \#1817 for SGv2/REST by supporting multi-dc setting for createKeyspace [\#1880](https://github.com/stargate/stargate/pull/1880) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- relates to \#1738: ported search weight and expression rules [\#1875](https://github.com/stargate/stargate/pull/1875) ([ivansenic](https://github.com/ivansenic))
- Initial implementation of Host-to-tenant metrics tagger [\#1872](https://github.com/stargate/stargate/pull/1872) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1862: making resume mode on the bridge optional [\#1870](https://github.com/stargate/stargate/pull/1870) ([ivansenic](https://github.com/ivansenic))
- relates to \#1737: final maven setup for the int test in Docs API v2 [\#1869](https://github.com/stargate/stargate/pull/1869) ([ivansenic](https://github.com/ivansenic))
- Allow resume mode to be settable on QueryParameters [\#1863](https://github.com/stargate/stargate/pull/1863) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1724: namespace resource [\#1859](https://github.com/stargate/stargate/pull/1859) ([ivansenic](https://github.com/ivansenic))

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-10...v2.0.0-ALPHA-11)

**Closed issues:**

- Use BatchQuery in document v2 write service [\#1846](https://github.com/stargate/stargate/issues/1846)
- Missing token should provide a response with body [\#1839](https://github.com/stargate/stargate/issues/1839)
- Collection REST Controller for the Document API V2 [\#1722](https://github.com/stargate/stargate/issues/1722)
- Implement `KeyspaceManager` based on the Stargate V2 common schema handling [\#1721](https://github.com/stargate/stargate/issues/1721)

**Merged pull requests:**

- relates to \#1737: proper way to skip integration tests [\#1861](https://github.com/stargate/stargate/pull/1861) ([ivansenic](https://github.com/ivansenic))
- removed example endpoint from the Docs V2 [\#1860](https://github.com/stargate/stargate/pull/1860) ([ivansenic](https://github.com/ivansenic))
- fixing dependency index for the guava usage [\#1857](https://github.com/stargate/stargate/pull/1857) ([ivansenic](https://github.com/ivansenic))
- Fix for \#1841 \(separate from SGv1\) [\#1856](https://github.com/stargate/stargate/pull/1856) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Allow overriding SGv1 REST and docsapi port via system property [\#1855](https://github.com/stargate/stargate/pull/1855) ([mpenick](https://github.com/mpenick))
- Change REST API module and artifact names to "sgv2-restapi" for Stargate V2 [\#1854](https://github.com/stargate/stargate/pull/1854) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#1839: api error response in case of a missing token [\#1852](https://github.com/stargate/stargate/pull/1852) ([ivansenic](https://github.com/ivansenic))
- Use BatchQuery in document v2 write service \(fixes \#1846\) [\#1851](https://github.com/stargate/stargate/pull/1851) ([olim7t](https://github.com/olim7t))
- Add JsonSchemaManager [\#1847](https://github.com/stargate/stargate/pull/1847) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1721: keyspace manager for Docs API v2 [\#1844](https://github.com/stargate/stargate/pull/1844) ([ivansenic](https://github.com/ivansenic))
- update quarkus to 2.9.1.Final [\#1843](https://github.com/stargate/stargate/pull/1843) ([ivansenic](https://github.com/ivansenic))
- closes \#1722: collection resource impl [\#1840](https://github.com/stargate/stargate/pull/1840) ([ivansenic](https://github.com/ivansenic))
- Implement WriteDocumentService in Document API v2 \(fixes \#1728\) [\#1833](https://github.com/stargate/stargate/pull/1833) ([olim7t](https://github.com/olim7t))
- Add JsonDocShredder, with tests [\#1831](https://github.com/stargate/stargate/pull/1831) ([EricBorczuk](https://github.com/EricBorczuk))
- relates to \#1737: setup for the integration tests in docs api v2  [\#1825](https://github.com/stargate/stargate/pull/1825) ([ivansenic](https://github.com/ivansenic))

## [v2.0.0-ALPHA-10](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-10) (2022-05-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-9...v2.0.0-ALPHA-10)

**Closed issues:**

- REST API should log ERROR for failure to contact Bridge gRPC service [\#1818](https://github.com/stargate/stargate/issues/1818)
- Re-enable old Stargate V1 REST API in v2.0.0 branch [\#1813](https://github.com/stargate/stargate/issues/1813)
- Adapt Document API JSON converter to gRPC based types [\#1727](https://github.com/stargate/stargate/issues/1727)
- Adapt Document API condition model to gRPC based types  [\#1726](https://github.com/stargate/stargate/issues/1726)
- Implement `TableManager` based on Stargate V2 common schema handling [\#1720](https://github.com/stargate/stargate/issues/1720)

**Merged pull requests:**

- Skip bridge authentication for GetSupportedFeatures \(fixes \#1821\) [\#1822](https://github.com/stargate/stargate/pull/1822) ([olim7t](https://github.com/olim7t))
- Change grpc bridge tenant header [\#1820](https://github.com/stargate/stargate/pull/1820) ([mpenick](https://github.com/mpenick))
- Log server-side Bridge/gRPC exceptions that are not client-induced [\#1819](https://github.com/stargate/stargate/pull/1819) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Port DocsApiUtils and tests [\#1816](https://github.com/stargate/stargate/pull/1816) ([EricBorczuk](https://github.com/EricBorczuk))
- Allow re-enabling of SGv1 REST API for SGv2 via system property [\#1814](https://github.com/stargate/stargate/pull/1814) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Migrate write query builders to Document API v2 [\#1810](https://github.com/stargate/stargate/pull/1810) ([olim7t](https://github.com/olim7t))
- eager creation of the stargate bridge for the request [\#1809](https://github.com/stargate/stargate/pull/1809) ([ivansenic](https://github.com/ivansenic))
- closes \#1720: table manager for creating, validating and deleting collections [\#1808](https://github.com/stargate/stargate/pull/1808) ([ivansenic](https://github.com/ivansenic))
- Use decorated name to cache keyspaces in bridge client [\#1807](https://github.com/stargate/stargate/pull/1807) ([olim7t](https://github.com/olim7t))
- relates to \#1720: simple cached schema management  [\#1806](https://github.com/stargate/stargate/pull/1806) ([ivansenic](https://github.com/ivansenic))
- Revisit value handling in the query builder [\#1805](https://github.com/stargate/stargate/pull/1805) ([olim7t](https://github.com/olim7t))
- Bumping version for next v2 release [\#1803](https://github.com/stargate/stargate/pull/1803) ([github-actions[bot]](https://github.com/apps/github-actions))
- Migrate JsonConverter to V2 [\#1797](https://github.com/stargate/stargate/pull/1797) ([EricBorczuk](https://github.com/EricBorczuk))
- Adapt Document API condition model to gRPC based types \(fixes \#1726\) [\#1796](https://github.com/stargate/stargate/pull/1796) ([olim7t](https://github.com/olim7t))
- Update startup and Docker scripts for GraphQL [\#1793](https://github.com/stargate/stargate/pull/1793) ([olim7t](https://github.com/olim7t))
- Port GraphQL schema-first to v2 architecture [\#1771](https://github.com/stargate/stargate/pull/1771) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-9](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-9) (2022-04-27)

[Full Changelog](https://github.com/stargate/stargate/compare/v1.0.55...v2.0.0-ALPHA-9)

**Merged pull requests:**

- Add utility to deal with TypeSpec instances [\#1794](https://github.com/stargate/stargate/pull/1794) ([olim7t](https://github.com/olim7t))
- closes \#1718: final configuration changes and fixes for Docs API V2 [\#1792](https://github.com/stargate/stargate/pull/1792) ([ivansenic](https://github.com/ivansenic))
- relates to \#1718: docs api configuration for the V2 [\#1783](https://github.com/stargate/stargate/pull/1783) ([ivansenic](https://github.com/ivansenic))
- Decouple bridge from public gRPC service \(fixes \#1770\) [\#1778](https://github.com/stargate/stargate/pull/1778) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-8](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-8) (2022-04-19)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-7...v2.0.0-ALPHA-8)

**Closed issues:**

- Bootstrap Document API V2 project with Quarkus [\#1717](https://github.com/stargate/stargate/issues/1717)
- Extend Bridge to support per-row page state and comparable bytes [\#1715](https://github.com/stargate/stargate/issues/1715)

**Merged pull requests:**

- Revert Persistence.unregisterEventListener \(fixes \#1777\) [\#1780](https://github.com/stargate/stargate/pull/1780) ([olim7t](https://github.com/olim7t))
- Add exception mappers for grpc status runtime exception and ErrorCodes [\#1779](https://github.com/stargate/stargate/pull/1779) ([EricBorczuk](https://github.com/EricBorczuk))
- relates to \#1717: correct grpc proto generation, exclude grpc-proto deps [\#1776](https://github.com/stargate/stargate/pull/1776) ([ivansenic](https://github.com/ivansenic))
- closes \#1719: added generic Quarkus observability for Document API V2 [\#1775](https://github.com/stargate/stargate/pull/1775) ([ivansenic](https://github.com/ivansenic))
- Add "enriched" responses to grpc bridge [\#1763](https://github.com/stargate/stargate/pull/1763) ([EricBorczuk](https://github.com/EricBorczuk))
- closes \#1717: boostratping the Docs API V2 with Quarkus [\#1760](https://github.com/stargate/stargate/pull/1760) ([ivansenic](https://github.com/ivansenic))
- Add bridge method to get supported persistence features [\#1757](https://github.com/stargate/stargate/pull/1757) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-7](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-7) (2022-04-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-6...v2.0.0-ALPHA-7)

**Merged pull requests:**

- Remove bridge token references [\#1766](https://github.com/stargate/stargate/pull/1766) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- 2.0.0-ALPHA-6 release [\#1765](https://github.com/stargate/stargate/pull/1765) ([github-actions[bot]](https://github.com/apps/github-actions))
- Port GraphQL CQL-first API to V2 architecture [\#1651](https://github.com/stargate/stargate/pull/1651) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-6](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-6) (2022-04-06)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-5...v2.0.0-ALPHA-6)

## [v2.0.0-ALPHA-5](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-5) (2022-03-23)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-4...v2.0.0-ALPHA-5)

## [v2.0.0-ALPHA-4](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-4) (2022-02-23)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-3...v2.0.0-ALPHA-4)

## [v2.0.0-ALPHA-3](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-3) (2022-02-14)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-2...v2.0.0-ALPHA-3)

**Closed issues:**

- Change Documents API port in StargateV2 to default to 8083 [\#1632](https://github.com/stargate/stargate/issues/1632)
- SGv2: disable old REST API [\#1625](https://github.com/stargate/stargate/issues/1625)
- Use streaming schema updates for Stargate V2 REST API [\#1553](https://github.com/stargate/stargate/issues/1553)

**Merged pull requests:**

- Port remaining resources to use StargateBridgeClient [\#1634](https://github.com/stargate/stargate/pull/1634) ([olim7t](https://github.com/olim7t))
- Fix \#1632: change Documents API default port to 8083 for StargateV2 [\#1633](https://github.com/stargate/stargate/pull/1633) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change "stargate.grpc.\*" config/system properties \(and related\) to "stargate.bridge.\*" [\#1631](https://github.com/stargate/stargate/pull/1631) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add schema read authorizations to the bridge [\#1630](https://github.com/stargate/stargate/pull/1630) ([olim7t](https://github.com/olim7t))
- Update startup scripts and docker images to incorporate bridge token [\#1629](https://github.com/stargate/stargate/pull/1629) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add delay when reconnecting to schema notifications [\#1628](https://github.com/stargate/stargate/pull/1628) ([olim7t](https://github.com/olim7t))
- Add configuration property for Docs API port [\#1627](https://github.com/stargate/stargate/pull/1627) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1625: comment out SGv1 REST API endpoints for SGv2 [\#1626](https://github.com/stargate/stargate/pull/1626) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next v2 release [\#1624](https://github.com/stargate/stargate/pull/1624) ([github-actions[bot]](https://github.com/apps/github-actions))
- Extract new StargateBridgeClient abstraction [\#1623](https://github.com/stargate/stargate/pull/1623) ([olim7t](https://github.com/olim7t))
- Bumping version for next release [\#1622](https://github.com/stargate/stargate/pull/1622) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v2.0.0-ALPHA-2](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-2) (2022-02-07)

[Full Changelog](https://github.com/stargate/stargate/compare/v2.0.0-ALPHA-1...v2.0.0-ALPHA-2)

**Merged pull requests:**

- Use infinite deadline for schema operations [\#1619](https://github.com/stargate/stargate/pull/1619) ([olim7t](https://github.com/olim7t))
- Refactor RestApiExtension to support integration testing of other API services [\#1616](https://github.com/stargate/stargate/pull/1616) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- fixed merge error that changed expected result string [\#1615](https://github.com/stargate/stargate/pull/1615) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- gRPC: Fix conversion of UDT field types [\#1613](https://github.com/stargate/stargate/pull/1613) ([olim7t](https://github.com/olim7t))
- Misc cleanup based on ErrorProne warnings [\#1611](https://github.com/stargate/stargate/pull/1611) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Improve Docker-compose scripts [\#1602](https://github.com/stargate/stargate/pull/1602) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add a test case to reproduce \#1577 fail on v2.0.0 [\#1591](https://github.com/stargate/stargate/pull/1591) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add tag/publish options to docker build script for v2 [\#1590](https://github.com/stargate/stargate/pull/1590) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bumping version for next release [\#1582](https://github.com/stargate/stargate/pull/1582) ([github-actions[bot]](https://github.com/apps/github-actions))
- Extract gRPC bridge as a separate bundle [\#1579](https://github.com/stargate/stargate/pull/1579) ([olim7t](https://github.com/olim7t))

## [v2.0.0-ALPHA-1](https://github.com/stargate/stargate/tree/v2.0.0-ALPHA-1) (2022-01-24)

_Note: this is a manually generated changelog for initial v2 Alpha release due to large number of changes on `v2.0.0` branch since its creation. This is approximately the same as the commit list for the `v2.0.0` branch on [GitHub](https://github.com/stargate/stargate/commits/v2.0.0) through 2022-01-24 (minus commits not tied to a PR)._

**Merged pull requests:**

- Another attempt at fix for #1580 (and undo #1567) (#1581)
- setting versioning for v2 (#1575)
- Handle bridge auth for background operations (#1557)
- Remove noisy logging statements that clutter perf tests (temporary SGv2 info) (#1576)
- Improve error message for the case of failed gRPC->external value conversion for REST API (#1578)
- Fix error handling in SchemaNotificationsHandler (#1564)
- fix quantiles check in the RestApiMetricsTest (#1571)
- Double-quote UDTs in type strings passed to QueryBuilder (#1569)
- Docker images for Stargate v2 (#1535)
- Fix issues with passing `null`s through grpc (#1566)
- Implement /metrics for SGv2/REST, change matching ITs to pass (#1551)
- Upgrade Micrometer and prometheus-client versions in v2.0.0 (#1560)
- Remove gRPC create operations (#1561)
- Update/Unify Jackson components used by SGv2 REST API to use 2.12.6 (over DW 2.10.5) (#1556)
- Fix gRPC to properly skip connection creation for GetSchemaNotifications (#1555)
- Add Tuple insert/get support in StargateV2/REST (#1541)
- Fix HealthCheck IT tests to work with StargateV2/REST rearchitecting (#1550)
- fixing errors in schema test case (#1549)
- disabling tests for features not currently supported in SGv2 (#1547)
- Fix an IT failing for some backends in CI by less strict message comparison (#1545)
- RestApiJWTAuthTest fails on SGv2 (#1542)
- gRPC: Add operation to stream schema changes (#1403)
- Add support for UDT inserts/gets in Stargate V2 REST API (#1537)
- Follow up to #1477: implement $exists, $contains, $containsKey and $containsEntry operations (#1523)
- Fix an IT that broke due to Varchar/Text changes (gRPC still internally uses Varchar) (#1534)
- Fix 3 ITs that now expect "text" as type for all columns, tables etc. (#1531)
- SGv2: implement "GET with where" for "Rows" endpoint (#1511)
- Support stringified values via REST API (#1509)
- (SGv2) Add GET method implementations (all, by-id) to UDT resource (#1498)
- Update remaining DropWizard dependencies, where applicable (#1495)
- Add SGv2 implementation of UDT endpoint (create, update, delete) (#1492)
- Exclude validation api, upgrade DropWizard (#1493)
- rework null handling in from codecs (#1485)
- make more use of conversions provided by Values class (#1482)
- Fix #1483: refactor SGv2 Tables, Keyspaces resource to use api/impl division (#1484)
- rework codec timestamps to use ISO string (#1473)
- Simplify error handling (#1474)
- Implement Column resource for SGv2/REST (#1472)
- Simplify gRPC stub construction in REST API(#1471)
- Add support for "order-by" feature of get-all-rows/get-row-by-pk (#1468)
- Extract gRPC stub creation to a filter (#1467)
- Implement index operations in sgv2-restapi (#1461)
- add support to v2 REST API for additional types (#1466)
- Implement patch/updateRow for SGv2/REST; passes a few more ITs (#1465)
- SGv2: replace OSS driver querybuilder with new sgv2 service-common QB (#1462)
- formatting (#1464)
- adding integration test using schema used in performance tests (#1460)
- SgV2: Separate "rows" endpoint into API interface, impl class (#1458)
- Add string-only QueryBuilder (#1448)
- Minor improvement to mapping of gRPC failures to HTTP Response codes (#1451)
- Fix StargateV2/REST ClusteringOrder enum return value (asc->ASC) (#1450)
- (WIP) Sgv2 #1435: Add createTable() (#1442)
- Fix stale schema issue in CreateTable operation (#1443)
- improved error handling on describe operations (#1436)
- Fix #1426: add SGv2/REST "getTable(s)" methods (#1433)
- Implement "delete rows" for SGv2 (#1429)
- Add "delete keyspace" REST impl for SGv2 (#1427)
- Fix #1422: add "getRows()" for SGv2 (#1423)
- Adding handling of indexes and materialized views to gRPC describe operations (#1413)
- Implement gRPC createTable operation (#1411)
- V2/jeff/grpc schema (#1409)
- WIP: use placeholder-INSERT for "addRow()" instead of as-JSON (#1407)
- Stargate v2 - draft PR for updates to gRPC API (#1398)
- Implement "addRow()" for SGv2 prototype, improve gRPC error mapping (#1402)
- Add "createKeyspace()" and "get[All]Keyspace[s]" schema REST endpoints for v2.0 prototype (#1394)
- Use OSS cassandra java QueryBuilder for "select all" implementation. (#1393)
- Stargate V2: complete the "getAllRows()" implementation with skeletal Proto-to-Java value converter (#1382)
- More complete (but partial) implementation of SGv2/REST/getAllRows() (#1369)
- V2/jeff/rest it updates (#1372)
- Rest integration test - initial work (#1359)
- Skeletal gRPC connection for "getAllRows()" (#1363)
- First skeletal version of SGv2 rest-service (#1357)

## Changelog history

For the older versions changelog, please have a look at [CHANGELOG_V1](./CHANGELOG_V1.md).


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
