# Stargate Documents API

The Documents API is built into the REST API module, an OSGi bundle that exposes various CRUD services over HTTP.
Specifically, the Documents API allows for storage of documents in the JSON format using C* tables (called "collections" in this context).

Here is a brief overview of the service (for more details, refer to the [Stargate
online docs]).

* There are endpoints for writing, updating, and deleting documents in [DocumentResource]
* For reading documents and searching through documents and collections, there are endpoints in [ReactiveDocumentResource]
* CRUD operations for collections exist in [CollectionsResource], and operations for modifying keyspace information (called "namespaces" in this context) are in [NamespacesResource]
* Operations for attaching a JSON Schema (see [Json schema info] for more details) are in [JsonSchemaResource]

## Entry points

[RestApiActivator] manages the starting and stopping of the OSGi bundle, which runs both the Documents API and the more general REST API.

[Server] is the HTTP server that exposes the Documents API services as REST resources. It is implemented with
[Dropwizard](https://www.dropwizard.io/en/latest/).

### Authentication

Every HTTP request performs token-based authentication. This is done by calling [Db]#getDocDataStoreForToken explicitly.
This method will either return an instance of [DocumentDB] which can be used for querying Cassandra and contains the authenticated subject,
or will throw an UnauthorizedException causing the appropriate 4XX response to be returned.

The DocumentDB gets stored in a simple cache (using [Caffeine](https://github.com/ben-manes/caffeine)) so the subsequent requests within a short
time-frame using the same token and headers need not suffer performance penalties.

### Resources

#### Document CUD operations

Note: All endpoints that take a body require a `Content-Type` header of `application/json`.

[DocumentResource] contains resources for creating, updating, and deleting documents.
If the document to be created is in a collection that doesn't yet exist, it will be created
before the document is inserted. Here is an overview of each endpoint:
* postDoc
  - Create a new document in a collection. A UUID will be assigned as the key for the new document.
* writeManyDocs
  - Create many new documents using a JSON array as the body. By default a UUID will be assigned as the key for each document, unless an `id-path` query param is specified.
* putDoc
  - Upsert a document. The key for the document will be the value of `:documentId`. This is idempotent and will overwrite any document that exists with the given key.
* patchDoc
  - Update a document by merging the keys that already exist with the new data. The key for the document will be the value of `:documentId`. This is idempotent but will not overwrite data that already exists, it will merge the data together instead.
* deleteDoc
  - Delete a document. Always returns a 204 response.
* putDocPath
  - Upsert data at a path in a document. This is idempotent and will overwrite any data that exists at that path in the document.
* patchDocPath
  - Update data at a path in a document by merging the keys that already exist with the new data. This is idempotent but will not overwrite data that already exists, it will merge the data together instead.
* deleteDocPath
  - Delete a path in a document. Always returns a 204 response.
    
[ReactiveDocumentResource] contains resources for reading and searching over documents.
It uses [RxJava](https://github.com/ReactiveX/RxJava) to perform reads in an asynchronous manner. The endpoints:
* getDocument
  - Get a full document by its key. If a `where` parameter is provided, returns matches within the document, as an array.
* getDocumentPath
  - Get the data at a path in a document by its key. If a `where` parameter is provided, returns matches at the document path, as an array.
* searchDoc
  - Get full documents that match the optional `where` parameter.

A more in-depth look at the `where` parameter can be found in the section called `Search Filter DSL` below.

[CollectionsResource] contains resources for CRUD operations on document collections (C* tables):
* getCollections
  - Returns all document collections in a particular namespace (i.e. "all document API tables in a keyspace").
    This includes information on schema updates that could be available for the collection.
* createCollection
  - Creates a collection. This can be used to "prime" a document insert for the first time, since the collection
    automatically gets created on first document write.
* deleteCollection
  - Deletes a collection, removing all data that was in the collection.
* upgradeCollection
  - Upgrades a collection. These upgrades are typically schema changes that will cause downtime, so be wary!

[NamespacesResource] contains resources for CRUD operations on document namespaces (C* keyspaces):
* getAllNamespaces
  - Returns info about all namespaces
* getOneNamespace
  - Returns info about one namespace
* createNamespace
  - Creates a new namespace.
* deleteNamespace
  - Deletes a namespace and all data within it.

[JsonSchemaResource] contains resources for attaching and removing JSON schemas from collections:
* attachJsonSchema
  - validates and adds a JSON schema to a collection. This will overwrite the previous schema if it exists.
* getJsonSchema
  - gets the JSON schema for a collection.

Once a schema is attached, all write operations must be done at the root of the document. In addition,
every write will be rejected if it does not conform to the given schema.

## Search Filter DSL

The resources found in [ReactiveDocumentResource] can use a `where` parameter in order to filter results.
This query parameter is valid JSON, and has data about the field to filter on, what operation to use, and the operand value.

e.g. `where={"name":{"$eq":"Bob"}}` (In practice, you would probably URL-encode this)

A few different features exist to match the field to something more granular:
* Dot-syntax to represent nested paths. For a document such as `{ "a": {"b": "c"}}"`
  you could use a where clause such as `where={"a.b": {"$eq": "c"}}`
* Bracket syntax to reference array paths. For a document such as `{ "a": ["c"]}"`
  you could use a where clause such as `where={"a.[0]": {"$eq": "c"}}`
* Asterisk glob syntax to match *any* path that is not an array. For two documents such as `{ "a": {"b": "c"}}` and `{"x": {"b": "d"}}`
  you could use a where clause such as `where={"*.b": {"$in": ["c", "d"]}}` to match both documents.
* Array glob syntax to match *any* path that is an array. For two documents such as `{ "a": ["c"]}` and `{"a": ["a", "d"]}`
  you could use a where clause such as `where={"a.[*]": {"$in": ["c", "d"]}}` to match both documents.
* Comma syntax to match multiple paths. For two documents such as `{ "a": ["c"]}` and `{"x": ["c", "d"]}`
  you could use a where clause such as `where={"a,x.[0]": {"$eq": "c"}}` to match both documents.
* Escape sequences using `\` for literal periods, commas, and asterisks

Many different filter operators can be used, each with different expected operand types and effects.
These are split into those operations that are "CQL supported" and those that require heavier in-memory filtering
to work properly:

CQL-supported operators:
* `$eq` - equals; expects a single text, numerical, or boolean value
* `$lt(e)` - less than (or equal); expects a single text or numerical value (text values are compared lexically by C*)
* `$gt(e)` - greater than (or equal); expects a single text or numerical value (text values are compared lexically by C*)
* `$exists` - a value exists for the field; expects only the value `true` (`$exists: false` does not work as of yet due to technical limitations)

CQL-unsupported operators:
* `$in` - matching any value in a list; expects a list of values e.g. `where={"name":{"$in":["Alice", "Bob"]}}`
* `$nin` - not matching any value in a list; expects a list of values just like $in
* `$and` - matching two different filter operators
* `$or` - matching at least one of many different filter operators

Using a CQL-unsupported operator with a CQL-supported operator is allowed, but will potentially involve bringing large amounts of data into memory
to fulfill the search.



## Testing

### Unit Tests
Unit tests live next to the code, in the [unit testing directory].

### Integration tests

The integration tests are located in the `testing` module, specifically in the package `io.stargate.it.http.docsapi`.
The main test cases for the API live in [BaseDocumentApiV2Test], and the test cases for the ancillary parts of the API live
in [NamespaceResourceIntTest], [CollectionsResourceIntTest], and [JsonSchemaResourceIntTest].

It is expected when adding or changing features that both Unit tests and Integration tests get changed or added, as appropriate.

[BaseDocumentApiV2Test]: testing/src/main/java/io/stargate/it/http/docsapi/BaseDocumentApiV2Test.java
[CollectionsResource]: src/main/java/io/stargate/web/docsapi/resources/CollectionsResource.java
[CollectionsResourceIntTest]: testing/src/main/java/io/stargate/it/http/docsapi/CollectionsResourceIntTest.java
[Db]: src/main/java/io/stargate/web/resources/Db.java
[DocumentDB]: src/main/java/io/stargate/web/docsapi/dao/DocumentDB.java
[DocumentResource]: src/main/java/io/stargate/web/docsapi/resources/DocumentResourceV2.java
[JsonSchemaResource]: src/main/java/io/stargate/web/docsapi/resources/JsonSchemaResource.java
[JsonSchemaResourceIntTest]: testing/src/main/java/io/stargate/it/http/docsapi/JsonSchemaResourceIntTest.java
[NamespacesResource]: src/main/java/io/stargate/web/docsapi/resources/NamespacesResource.java
[NamespaceResourceIntTest]: testing/src/main/java/io/stargate/it/http/docsapi/NamespaceResourceIntTest.java
[ReactiveDocumentResource]: src/main/java/io/stargate/web/docsapi/resources/ReactiveDocumentResourceV2.java
[RestApiActivator]: src/main/java/io/stargate/web/RestApiActivator.java
[Server]: src/main/java/io/stargate/web/impl/Server.java

[Stargate online docs]: https://stargate.io/docs/stargate/1.0/quickstart/quick_start-document.html
[Json schema info]: https://json-schema.org/
[unit testing directory]: src/test/java