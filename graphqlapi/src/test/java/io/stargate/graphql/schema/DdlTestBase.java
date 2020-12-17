package io.stargate.graphql.schema;

import graphql.schema.GraphQLSchema;

public abstract class DdlTestBase extends GraphQlTestBase {

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService, authorizationService);
  }
}
