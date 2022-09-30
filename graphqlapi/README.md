# Stargate GraphQL

This module builds an OSGi bundle that exposes Stargate's GraphQL services over HTTP.

Here is a brief functional overview of those services (for more details, refer to the [Stargate
online docs]).

* `/graphql-schema` exposes DDL operations (describe, create table, etc). It can be used for any
  keyspace (most operations take a `keyspace` argument).
* for each keyspace, there is a `/graphql/<keyspace>` service for DML operations (read and insert
  data, etc). Initially, its GraphQL schema is automatically generated from the CQL data model. We
  call this the **CQL-first** model.
* `/graphql-admin` allows users to deploy their own GraphQL schemas. Stargate will alter the CQL
  schema to match what they want to map. We call this the **GraphQL-first** model. Once a
  GraphQL-first schema has been deployed in this manner, it replaces the CQL-first one:
  `/graphql/<keyspace>` now uses the custom schema, and the generated schema is not available
  anymore.

## Entry points

[GraphqlActivator] manages the starting and stopping of the OSGi bundle.

[DropwizardServer] is the HTTP server that exposes the GraphQL services as REST resources. As the
name indicates, it's implemented with
[Dropwizard](https://www.dropwizard.io/en/latest/).

## GraphQL Java primer

We rely extensively on [GraphQL Java](https://www.graphql-java.com/). Before delving further into
the Stargate code, it can be helpful to have a basic understanding of that library:

```java
Random random = new Random();
GraphQLSchema schema =
    GraphQLSchema.newSchema() // (1)
        .query(
            GraphQLObjectType.newObject()
                .name("Query")
                .field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name("random")
                        .type(Scalars.GraphQLInt)
                        .build())
                .build())
        .codeRegistry( // (2)
            GraphQLCodeRegistry.newCodeRegistry()
                .dataFetcher(
                    FieldCoordinates.coordinates("Query", "random"),
                    (DataFetcher<Integer>) environment -> random.nextInt())
                .build())
        .build();

GraphQL graphql = GraphQL.newGraphQL(schema).build(); // (3)

ExecutionResult result = graphql.execute("{ random }");
System.out.println(result.getData().toString()); // prints {random=1384094011}
```

1. `newSchema()` provides a DSL to create the GraphQL schema programmatically. This example will
   produce:

    ```graphql
    type Query {
      random: Int
    }
    ```
2. The code registry provides the logic to execute queries at runtime. It is broken down into data
   fetchers. Each fetcher handles a field, identified by its coordinates in the schema.
3. Finally, we turn the `GraphQLSchema` into an executable `GraphQL`.


## HTTP layer

### Authentication

[AuthenticationFilter] intercepts every HTTP request to perform token-based authentication. The
subject is stored in the HTTP context for consumption in the REST resources.

We also take advantage of this filter to create a single, reusable `DataStore` instance for the
request (this used to be done in the data fetchers, but it lead to performance issues, see
[#1021](https://github.com/stargate/stargate/issues/1021) and
[#1034](https://github.com/stargate/stargate/pull/1034)).

### Resources

#### GraphQL services

They are implemented as REST resources. They all extend [GraphqlResourceBase], which handles the
various ways to query GraphQL over HTTP: POST vs GET, arguments in the query string vs the body,
multipart, etc.

The only thing that changes across subclasses is how we get hold of the `GraphQL` object to query
(see [GraphQL layer](#graphql-layer) below), and whether multipart is supported.

[StargateGraphqlContext] allows us to inject state that will be available later in the data fetchers
(see [CassandraFetcher]). In particular, this is how we pass the authentication subject and
datastore that were initialized in the authentication filter. The context also handles
[batching](#batching), which will be described below.

#### Other resources

[PlaygroundResource] exposes the GraphQL playground (an in-browser client, served from a static HTML
file). [FilesResource] provides downloadable versions of users' custom schemas, and CQL directive
definitions.


## GraphQL layer

[GraphqlCache] provides the `GraphQL` instances used by the HTTP layer. The lifecycle of those
objects varies depending on the service:

### CQL-first

#### DDL (`/graphql-schema`)

The DDL service never changes, we only need to cache one instance. The schema is built by
[DdlSchemaBuilder].

The data fetchers are in the package [cqlfirst.ddl.fetchers]. Their implementation is pretty
straightforward: translate the GraphQL operations into CQL queries for the persistence layer.

Our fetchers are generally loosely typed, a lot of them return raw data as a `Map<String, Object>`.
But there is a case where that doesn't work, and that's when one of the inner fields can be
parameterized:

```graphql
query {
  keyspace(name: "library") {
    books: table(name: "books") { columns { name } }
    authors: table(name: "authors") { columns { name } }
  }
}
```

In that situation, there is no way to return both tables with a `Map<String, Object>`, because the
map keys are the actual field types, not their aliases. The solution is to use a strongly-typed DTO,
such as [KeyspaceDto]. (In hindsight, it might have been better to use this approach more often, but
the current version works so there's no strong need to change it.)

#### DML (`/graphql/<keyspace>`)

We cache one `GraphQL` per keyspace.

The schema is built by [DmlSchemaBuilder]. This time things are a bit more dynamic, we read from a
`Keyspace` and generate the schema accordingly. For example if there is a `"User"` table, we'll have
a `User` GraphQL type, an `insertUser` operation, etc.

The schema often references the same types over and over. For example, each time a table can be
queried by an `int`, the query operation will have an `IntFilterInput` argument that defines various
search operators (`eq`, `lte`, etc). In the builder code, every occurrence of a given type must be
represented by the same `GraphQLType` instance, so we must keep track of the types we have generated
so far, and reuse them if they appear again. This is handled by [FieldTypeCache] and its subclasses.

We name our GraphQL types and fields after CQL tables and columns. But GraphQL identifiers have more
restrictive naming rules, so we sometimes need to convert names. This is covered by [NameMapping],
which also caches the results.

CQL also has a wider range of primitive types. [CqlScalar] defines a number of custom GraphQL
scalars to match the CQL types that have no direct equivalent. See the `*Coercing` classes for the
details of the type coercion rules.

The data fetchers are in the package [cqlfirst.dml.fetchers]. There is one fetcher per type of
query, it gets initialized with the `Table` that this particular operation was created for.

The DML schema listens to CQL schema change events in order to adapt in real time. See
`GraphqlCache.onKeyspaceChanged()`, which gets registered in the [GraphqlCache] constructor.

It's also worth noting that both versions of the API share the same cache entry: if a GraphQL-first
schema was deployed for this keyspace, we use it, otherwise we generate a CQL-first schema. See the
logic in `GraphqlCache.getDml()`.

### GraphQL-first

#### Admin (`/graphql-admin`)

The admin service never changes, we only need to cache one instance. The schema is built by
[AdminSchemaBuilder].

The data fetchers are in the package [graphqlfirst.fetchers.admin]. Most of the fetchers are
trivial, with the exception of [DeploySchemaFetcherBase]: this is where we process a custom GraphQL
schema received from the user.

* we first check if a previous custom schema was deployed for this keyspace. This is stored in a
  table `stargate_graphql.schema_source`. We also use a lightweight transaction as a rudimentary
  concurrency control mechanism to ensure that two deployments cannot run concurrently. This is all
  handled by [SchemaSourceDao].
* [SchemaProcessor] parses the user's GraphQL schema, and builds:
  * [MappingModel], a representation of the equivalent CQL model. This starts in
    `MappingModel.build()`, and then branches out to `*ModelBuilder` helper classes for each kind of
    GraphQL element (types, operations, etc).
  * the `GraphQL` instance. We use the user's schema directly (it is valid if we got this far), and
    [MappingModel] generates the data fetchers.
* [CassandraMigrator] checks if that theoretical CQL model matches the actual contents of the
  database. If it doesn't, there are various user-configurable migration strategies to handle the
  differences: error out, or try to alter the CQL model. In the latter case, the check returns a list
  of migration queries, that we can now execute.
* finally, we insert the new version in `stargate_graphql.schema_source`. The new `GraphQL` instance
  is sent to [GraphqlCache], which starts to serve it to its clients.

We provide a set of CQL directives that allow users to control certain aspects of their mapping (for
example, use a different table name than the inferred default). They are defined in [CqlDirectives],
and referenced throughout the model-building code. You can also download a text version from a
running Stargate instance at `graphql-files/cql_directives.graphql`.

#### Deployed (`/graphql/<keyspace>`)

Once a user has deployed their own GraphQL schema, it replaces the CQL-first schema for that
keyspace. As already mentioned, [GraphqlCache] contains logic to determine which variant to load.

There are two ways that a GraphQL-first `GraphQL` instance can be created:

* if the deploy operation just happened in this Stargate process, then [DeploySchemaFetcherBase]
  already has the `GraphQL`, and puts it directly in the cache with `GraphqlCache.putDml`.
* if the deployment happened via another Stargate instance, or if Stargate just restarted, we need
  to reload the schema from `stargate_graphql.schema_source`. This uses a simplified version of the
  deployment process described in the previous section, see
  `GraphqlCache.LazySchemaFirstGraphqlHolder`.

Unlike CQL-first, GraphQL-first does not react to external CQL schema changes: the user is supposed
to have full control over the GraphQL schema, so we can't just change it behind their back. So our
assumption is that once users go GraphQL-first, they will evolve their CQL schema via successive
deploy operations, not by directly altering the database. If things get out of sync, there are no
guarantees: queries might start to fail, or a saved schema might fail to reload.

The data fetchers are in the package [graphqlfirst.fetchers.deployed]. They rely on the
representation that was built by [MappingModel]. For example, the query fetcher relies on a query
model that defines the target table, which parameters of the GraphQL operation will be mapped to
`WHERE` clauses, etc. This part of the code is quite tedious because our mapping rules are very
flexible: there are many different ways to define operations and map the results. The best way to
get familiar with a fetcher is to look at the integration tests and trace their execution.

### Batching

Batching is a cross-cutting concern, both CQL-first and GraphQL-first support the `@atomic`
directive to indicate that a set of mutations must be executed as a single CQL batch:

```graphql
mutation @atomic {
  ... // mutations
}
```

[StargateGraphqlContext] (via its inner class `BatchContext`) provides coordination services that
allows independent fetchers to accumulate queries, and track which is the last one that must execute
the batch.


## Testing

### Manually

The easiest way is to use the built-in GraphQL playground at http://localhost:8080/playground.

You can then interact with the various GraphQL schemas by entering their URL in the playground's
address bar, for example http://localhost:8080/graphql/{keyspace}.

### Integration tests

The integration tests are located in the [testing] module. There use two different approaches:

* the tests that extend [ApolloTestBase] use the [Apollo client] library. They provide a good
  example of using a "real-world" client, but require a lot of boilerplate: external definition
  files, code generation...
* the other tests use a more lightweight approach that takes GraphQL queries as plain strings, and
  returns raw JSON responses. See [GraphqlClient] and its subclasses.

If you add new tests, please favor the lightweight approach.

[AdminSchemaBuilder]: src/main/java/io/stargate/graphql/schema/graphqlfirst/AdminSchemaBuilder.java
[ApolloTestBase]: ../testing/src/main/java/io/stargate/it/http/graphql/cqlfirst/ApolloTestBase.java
[AuthenticationFilter]: src/main/java/io/stargate/graphql/web/resources/AuthenticationFilter.java
[CassandraFetcher]: src/main/java/io/stargate/graphql/schema/CassandraFetcher.java
[CassandraMigrator]: src/main/java/io/stargate/graphql/schema/graphqlfirst/migration/CassandraMigrator.java
[CqlDirectives]: src/main/java/io/stargate/graphql/schema/graphqlfirst/processor/CqlDirectives.java
[CqlScalar]: src/main/java/io/stargate/graphql/schema/scalars/CqlScalar.java
[DdlSchemaBuilder]: src/main/java/io/stargate/graphql/schema/cqlfirst/ddl/DdlSchemaBuilder.java
[DeploySchemaFetcherBase]: src/main/java/io/stargate/graphql/schema/graphqlfirst/fetchers/admin/DeploySchemaFetcherBase.java
[DmlSchemaBuilder]: src/main/java/io/stargate/graphql/schema/cqlfirst/dml/DmlSchemaBuilder.java
[DropwizardServer]: src/main/java/io/stargate/graphql/web/DropwizardServer.java
[FieldTypeCache]: src/main/java/io/stargate/graphql/schema/cqlfirst/dml/FieldTypeCache.java
[FilesResource]: src/main/java/io/stargate/graphql/web/resources/FilesResource.java
[GraphqlActivator]: src/main/java/io/stargate/graphql/GraphqlActivator.java
[GraphqlCache]: src/main/java/io/stargate/graphql/web/resources/GraphqlCache.java
[GraphqlClient]: ../testing/src/main/java/io/stargate/it/http/graphql/GraphqlClient.java
[GraphqlResourceBase]: src/main/java/io/stargate/graphql/web/resources/GraphqlResourceBase.java
[KeyspaceDto]: src/main/java/io/stargate/graphql/schema/cqlfirst/ddl/fetchers/KeyspaceDto.java
[MappingModel]: src/main/java/io/stargate/graphql/schema/graphqlfirst/processor/MappingModel.java
[NameMapping]: src/main/java/io/stargate/graphql/schema/cqlfirst/dml/NameMapping.java
[PlaygroundResource]: src/main/java/io/stargate/graphql/web/resources/PlaygroundResource.java
[SchemaProcessor]: src/main/java/io/stargate/graphql/schema/graphqlfirst/processor/SchemaProcessor.java
[SchemaSourceDao]: src/main/java/io/stargate/graphql/persistence/graphqlfirst/SchemaSourceDao.java
[StargateGraphqlContext]: src/main/java/io/stargate/graphql/web/StargateGraphqlContext.java
[cqlfirst.ddl.fetchers]: src/main/java/io/stargate/graphql/schema/cqlfirst/ddl/fetchers
[cqlfirst.dml.fetchers]: src/main/java/io/stargate/graphql/schema/cqlfirst/dml/fetchers
[graphqlfirst.fetchers.admin]: src/main/java/io/stargate/graphql/schema/graphqlfirst/fetchers/admin
[graphqlfirst.fetchers.deployed]: graphqlapi/src/main/java/io/stargate/graphql/schema/graphqlfirst/fetchers/deployed
[testing]: ../testing/src/main/java/io/stargate/it/http/graphql

[Apollo client]: https://www.apollographql.com/docs/android/essentials/get-started-java/
[Stargate online docs]: https://stargate.io/docs/stargate/1.0/quickstart/quick_start-graphql.html
