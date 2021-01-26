# Changelog

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
