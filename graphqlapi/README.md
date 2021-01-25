# GraphQL for Stargate

Easy to use APIs for extending Stargate to access data using graphql. 

These APIs are used as a Stargate OSGi bundle. Add the graphql jar to the Stargate plugin folder to use.

## Getting Started

### Installation
To build the GraphQL plugin individually, run the command:

```sh
./mvnw package -pl graphqlapi
```

### Using GraphQL

By default, a GraphQL endpoint is started and will generate a GraphQL schema per 
keyspace. You need at least one user-defined keyspace in your database to get 
started.  

# GraphQL API for Apache Cassandra

### Using the Playground

The easiest way to get started is to use the built-in GraphQL playground. GraphQL
playground can be downloaded as a standalone app [here](https://github.com/prisma-labs/graphql-playground)
and can then be accessed by going to http://localhost:8080/graphql/{keyspace}. 
Once in the playground, you can create new schema and interact with the GraphQL APIs.

### Path Layout

By default, the server paths are structured to provide:
* `/graphql-schema`: An API for exploring and creating schema, in
database terminology this is know as: Data Definition Language (DDL). In
Cassandra these are the queries used to create, modify, drop keyspaces and
tables e.g. `CREATE KEYSPACE ...`, `CREATE TABLE ...`, `DROP TABLE ...`.
* `/graphql/<keyspace>`: An API for querying and modifying your Cassandra
tables using GraphQL fields.

#### Keyspaces

For each keyspace created in your Cassandra schema, a new path is created under
the root `graphql-path` (default is: `/graphql`). For example, a path
`/graphql/library` is created for the `library` keyspace when it is added to
the Cassandra schema.


### Schema

Before you can get started using GraphQL APIs you must create a keyspace and at
least one table. If your Cassandra database already has existing schema then the
server has already imported your schema and you might skip this step.
Otherwise, use the following steps to create new schema.

Inside the playground, navigate to http://localhost:8080/graphql-schema, then
create a keyspace by executing:

```graphql
mutation {
  createKeyspace(
    name:"library", # The name of your keyspace
    # Controls how your data is replicated,
    dcs: { name:"dc1", replicas: 1 }  # Use at least 3 replicas in production
  )
}
```

After the keyspace is created you can create tables by executing:

```graphql
mutation {
  books: createTable(
    keyspaceName:"library", 
    tableName:"books", 
    partitionKeys: [ # The keys required to access your data
      { name: "title", type: {basic: TEXT} }
    ]
    values: [ # The values associated with the keys
      { name: "author", type: {basic: TEXT} }
    ]
  )
  authors: createTable(
    keyspaceName:"library", 
    tableName:"authors", 
    partitionKeys: [
      { name: "name", type: {basic: TEXT} }
    ]
    clusteringKeys: [ # Secondary key used to access values within the partition
      { name: "title", type: {basic: TEXT} }
  	]
  )
}
```

You can also create the schema using `cqlsh` and the server will automatically
apply your schema changes.

```cql
CREATE KEYSPACE library WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'};

CREATE TABLE library.books (
    title text PRIMARY KEY,
    author text
);

CREATE TABLE library.authors (
    name text,
    title text,
    PRIMARY KEY (name, title)
) WITH CLUSTERING ORDER BY (title ASC);
```

#### Conversion warnings

If a GraphQL API is not generated for an existing CQL table schema then there might have been a
failure with the conversion. The failure will create a warning that can be retrieved using the
`conversionWarnings` operation under the table's keyspace URL:
`http://localhost/graphql/<keyspace>`. 

To query the conversion warnings use:

```graphql
query {
  conversionWarnings
}
```

#### Schema queries
You can also query the schema for tables and their types
```graphql
query {
  keyspace(name: "library") {
    name
    table(name: "books") {
      name
      columns {
        name
        type {
          basic
        }
      }
    }
  }
}
```

#### Adding columns
```graphql
mutation {
  alterTableAdd(
    keyspaceName:"library",
    tableName:"books",
    toAdd:[{
      name: "isbn",
      type: {
        basic: TEXT
      }
    }]
  )
}
```

#### Removing columns
```graphql
mutation {
    alterTableDrop(
    keyspaceName:"library",
    tableName:"books",
    toDrop:["isbn"]
  )
}
```

#### Dropping tables
```graphql
mutation {
  dropTable(keyspaceName:"library",
    tableName:"books")
}
```

### API Generation

For each table in your Cassandra schema, several fields are created for handling
queries and mutations. For example, the GraphQL API generated for the `books`
table looks like this:

```graphql
schema {
  query: Query
  mutation: Mutation
}

type Query {
  books(value: BooksInput, filter: BooksFilterInput, orderBy: [BooksOrder], options: QueryOptions): BooksResult
  booksFilter(filter: BooksFilterInput!, orderBy: [BooksOrder], options: QueryOptions): BooksResult
}

type Mutation {
  insertBooks(value: BooksInput!, ifNotExists: Boolean, options: UpdateOptions): BooksMutationResult
  updateBooks(value: BooksInput!, ifExists: Boolean, ifCondition: BooksFilterInput, options: UpdateOptions): BooksMutationResult
  deleteBooks(value: BooksInput!, ifExists: Boolean, ifCondition: BooksFilterInput, options: UpdateOptions): BooksMutationResult
}
```

#### Queries:

* `books()`: Query book values by equality. If no `value` argument is provided
  then the first 100 (default pagesize) values are returned.

* `booksFilter`: (**Deprecated**) Query book values by filtering the result with additional
  operators e.g. `gt` (greater than), `lt` (less than), `in` (in a list of
  values) etc. The `books()` equality style query is preferable if your queries
  don't require the use non-equality operators.

#### Mutations:
  
* `insertBooks()`: Insert a new book. This is an "upsert" operation that will
  update the value of existing books if they already exists unless `ifNotExists`
  is set to `true`. Using `ifNotExists` causes the mutation to use a lightweight
  transaction (LWT) adding significant overhead.

* `updateBooks()`: Update an existing book. This is also an "upsert" and will
  create a new book if one doesn't exists unless `ifExists` is set to `true`.
  Using `ifExists` or `ifCondition` causes the mutation to use a lightweight
  transaction (LWT) adding significant overhead.

* `deleteBooks()`: Deletes a book.  Using `ifExists` or `ifCondition` causes the
   mutation to use a lightweight transaction (LWT) adding significant overhead.

As more tables are added to a keyspace additional fields will be added to the
`Query` and `Mutation` types to handle queries and mutations for those
new tables.

### API Naming Convention

The default naming convention converts CQL (tables and columns) names to
`lowerCamelCase` for GraphQL fields and `UpperCamelCase` for GraphQL types. If
the naming convention rules result in a naming conflict, a number suffix is
appended to the name e.g. `someExistingColumn` --> `someExistingColumn2`. If a
naming conflict is not resolved within the maximum suffix value of `999` it will
result in a error.

### Using the API

This section will show you how to add and query books. Navigate to your keyspace
inside the playground by going to http://localhost:8080/graphql/library and add
some entries.

#### Insert Books

```graphql
mutation {
  moby: insertBooks(value: {title:"Moby Dick", author:"Herman Melville"}) {
    value {
      title
    }
  }
  catch22: insertBooks(value: {title:"Catch-22", author:"Joseph Heller"}) {
    value {
      title
    }
  }
}
```

#### Query Books

To query those values you can run the following:

```graphql
query {
    books {
      values {
      	title
      	author
      }
    }
}
```

```json
{
  "data": {
    "books": {
      "values": [
        {
          "author": "Joseph Heller",
          "title": "Catch-22"
        },
        {
          "author": "Herman Melville",
          "title": "Moby Dick"
        }
      ]
    }
  }
}
```

#### Query a Single Book

A specific book can be queried by providing a key value:

```graphql
query {
    books (value: {title:"Moby Dick"}) {
      values {
      	title
      	author
      }
    }
}
```

```json
{
  "data": {
    "books": {
      "values": [
        {
          "author": "Herman Melville",
          "title": "Moby Dick"
        }
      ]
    }
  }
}
```

## Using Apollo Client

This is a basic guide for getting started with Apollo Client 2.x in Node. First
you'll need to install dependencies. These examples also utilize the `books`
schema created in the schema section.

### Node

```sh
$ npm init # Follow prompts
$ npm install apollo-client apollo-cache-inmemory apollo-link-http \
  apollo-link-error apollo-link graphql graphql-tag node-fetch --save
```

Copy this into a file of your chose or your `main` entry point, usually
`index.js`.

```js
const { HttpLink } = require('apollo-link-http')
const { InMemoryCache } = require('apollo-cache-inmemory')
const { ApolloClient } = require('apollo-client')
const fetch = require('node-fetch')
const gql = require('graphql-tag')

const client = new ApolloClient({
  link: new HttpLink({
    uri: 'http://localhost:8080/graphql/library',
    fetch: fetch
  }),
  cache: new InMemoryCache()
})

const query = 
client.query({ 
  query: gql`
    {
       books {
         values {
           author
         }
       }
    }
  `
}).then(result => {
  console.log(result)
})
```

Then run then example.

```sh
$ node index.js # Use the name of the file you created in the previous step
{
  data: { books: { values: [Array], __typename: 'BooksResult' } },
  loading: false,
  networkStatus: 7,
  stale: false
}
```


## API Features

### Query Options

Query field operations have an `options` argument which can be used to control
the behavior queries.

```
input QueryOptions {
  consistency: QueryConsistency
  limit: Int
  pageSize: Int
  pageState: String
}
```

#### Consistency

Query consistency controls the number of replicas that must agree before returning
your query result. This is used to tune the balance between data consistency and
availability for reads. `SERIAL` and `LOCAL_SERIAL` are for use when doing
conditional inserts and updates which are also known as lightweight transactions
(LWTs), they are similar to `QUORUM` and `LOCAL_QUORUM`, respectively. More
information about read consistency levels can be found in [How is the
consistency level configured?].

```graphql
enum QueryConsistency {
  LOCAL_ONE    # Only wait for one replica in the local data center
  LOCAL_QUORUM # Wait for a quorum, `floor(total_replicas / 2 + 1)`, of replicas in the local data center
  ALL          # Wait for all replicas in all data centers
  SERIAL       # Used to read the latest value checking for inflight updates
  LOCAL_SERIAL # Same as `SERIAL, but only in the local data center
}
```

#### Limit

Limit sets the maximum number of values a query returns. 

#### PageSize and PageState

Query paging can be controlled by modifying the values of `pagingSize` and
`pageState` in the input type `QueryOptions` argument. The default `pageSize` is
100 values.


The `pageState` is returned in the data result of queries. It is a marker that
can be passed to subsequent queries to get the next page.

``` graphql
query {
    books (options:{pageSize: 10}) {
      values {
        # ...
      }
      pageState # Return the page state
    }
}
```

The `pageState` value is returned in the result:

```json
{
  "data": {
    "books": {
      "pageState": "CENhdGNoLTIyAPB////+AA==",
      "values": [
        ...
      ]
    }
  }
}
```

`pageState` from the previous result can be passed into a followup query to get
the next page:

```graphql
query {
    books (options:{pageSize: 10, pageState: "CENhdGNoLTIyAPB////+AA=="}) {
      # ...
    }
}
```

### Order By

The order of values returned can be controlled by passing an enumeration value
argument, `orderBy`, for the given field.

Given the schema below this would return the books written by `"Herman
Melville"` in order of his books by page length.

```graphql
query {
  bookBySize(value:{author:"Herman Melville"}, orderBy: pages_DESC) {
    values {
      title
      pages
    }
  }
}
```

Each query field has an `orderBy` argument and a specific order enumeration
type, in this case, `BookBySizeOrder`.

```graphql
type Query {
  bookBySize(options: QueryOptions, value: BookBySizeInput, orderBy: [BookBySizeOrder]): BookBySizeResult
  
  # ...
}

enum BookBySizeOrder {
  author_ASC
  author_DESC
  pages_ASC
  pages_DESC
  title_ASC
  title_DESC
}

# ...

```

### Filtering

Filter queries allow for the use of additional operators to control which values
are returned. 

The `filter` parameter allows for using more flexible conditional operators,
`eq` (equal), `gte` (greater than, or equal), `lte` (less than, or equal), etc.
This query returns all the books by "Herman Melville" with a length between 100
and 800 pages.

```graphql
query {
  bookBySize(filter:{author: {eq: "Herman Melville"}, pages: {gte: 100, lte: 800}}) {
    values {
      title
      pages
    }
  }
}
```

The `in` operator allow for filtering a specific set of values. This query
returns the books by "Herman Melville` in the provided `in` set.

```graphql
query {
  bookBySize(filter:{author: {eq: "Herman Melville"}, title: {in: ["Moby Dick", "Redburn"]}) {
    values {
      title
      pages
    }
  }
}
```

#### Supported Operators

| Operator | Example | Description | 
| --- | --- | --- |
| `eq` | `filter: { title: { eq: "Moby Dick" }`  | Equals              |
| `ne` | `filter: { title: { ne: "Moby Dick" }`  | Not equals          |
| `lt` | `filter: { pages: { lt: 800 }`          | Less than           |
| `lte`| `filter: { pages: { lte: 799 }`         | Less than equals    |
| `gt` | `filter: { pages: { gt: 100 }`          | Greater than        |
| `gte`| `filter: { pages: { gte: 99 }`          | Greater than equal  |
| `in` | `title: {in: ["Moby Dick", "Redburn"]}` | In a list of values |


### Mutation Options

Mutation field operations have an `options` argument which can be used to control
the behavior mutations (inserts, updates, and deletes).

```graphql
input MutationOptions {
  consistency: MutationConsistency
  serialConsistency: SerialConsistency
  ttl: Int = -1
}
```

#### Consistency

Mutation consistency controls the number of replicas that must acknowledge a
mutation before returning. This is used to tune the balance between data
consistency and availability for writes. More information about write
consistency levels can be found in [How is the consistency level configured?].


```graphql
enum MutationConsistency {
  LOCAL_ONE    # Acknowledge only a single replica in the local data center
  LOCAL_QUORUM # Acknowledge a quorum of replicas in the local data center
  ALL          # Acknowledge all replicas
}
```

#### Serial Consistency

Serial consistency is used in conjunction with conditional inserts and updates
(LWTs) and in all other mutations types it is ignored if provided.

```graphql
enum SerialConsistency {
  SERIAL       # Linearizable consistency for conditional inserts and updates
  LOCAL_SERIAL # Same as `SERIAL`, but local to a single data center
}
```

#### Time-to-live (TTL)

Time-to-live (TTL), defined in seconds, controls the amount of time a value
lives in the database before expiring e.g. `ttl: 60` means the associated values
are no longer readable after 60 seconds. More information about TTL can be found
in [Expiring data with time-to-live].

### Conditional Inserts, Updates, and Deletes

Conditional mutations are mechanism to add or modify field values only when a
provided condition, `ifExists`, `ifNotExists, and `ifCondition`, is satisfied.
These conditional mutations require the use lightweight transactions (LWTs)
which are significantly more expensive than regular mutations. More information
about LWTs can be found in [Using lightweight transactions].


The following book will only be added if an existing entry does not exist. The
`applied` field can be used to determine if the mutation succeeded. If the
mutation succeeded then `applied: true` is returned.

```graphql
mutation {
  insertBooks(value: {title: "Don Quixote", author: "Miguel De Cervantes"}, ifNotExists: true) {
    applied
    value {
      title
      author
    }
  }
}
```

Result:

```json
{
  "data": {
    "insertBooks": {
      "applied": true,
      "value": {
        "author": "Miguel De Cervantes",
        "title": "Don Quixote"
      }
    }
  }
}
```

#### Return values

When mutations fail with `applied: false`, the most up-to-date, existing values
are returned in the result. Using the previous query, if the book already exists
then the result would return a value for the `author`. Values that part of the
paritition and clustering keys are always returned in the result, independent of
whether the mutation was applied.

```graphql
mutation {
  insertBooks(value: {title: "Don Quixote", author: "Herman Melville"}, ifNotExists: true) {
    applied
    value {
      title
      author
    }
  }
}
```

Result:

```json
{
  "data": {
    "insertBooks": {
      "applied": false,
      "value": {
        "author": "Miguel De Cervantes",
        "title": "Don Quixote"
      }
    }
  }
}
```

[Expiring data with time-to-live]: https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useExpire.html
[Using lightweight transactions]: https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/dml/dmlLtwtTransactions.html
[How is the consistency level configured?]: https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/dml/dmlConfigConsistency.html
