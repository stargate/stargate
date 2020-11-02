package io.stargate.graphql.schema;

import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthnzService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Keyspace;

/** Single entry point to obtain GraphQL schemas. */
public class SchemaFactory {

  /**
   * Builds the GraphQL schema to query and modify data for a particular CQL keyspace.
   *
   * <p>This is the API exposed at {@code /graphql/<keyspaceName>}.
   */
  public static GraphQLSchema newDmlSchema(
      Persistence persistence, AuthnzService authnzService, Keyspace keyspace) {
    return new DmlSchemaBuilder(persistence, authnzService, keyspace).build();
  }

  /**
   * Builds the GraphQL schema to manipulate the Cassandra data model, in other words create, remove
   * or alter keyspaces, tables, etc.
   *
   * <p>This is the API exposed at {@code /graphql-schema}.
   */
  public static GraphQLSchema newDdlSchema(Persistence persistence, AuthnzService authnzService) {
    return new DdlSchemaBuilder(persistence, authnzService).build();
  }
}
