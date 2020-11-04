package io.stargate.graphql.schema;

import graphql.schema.GraphQLSchema;

public abstract class DdlTestBase extends GraphQlTestBase {
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService);
  }
}
