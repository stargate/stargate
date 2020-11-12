package io.stargate.graphql.schema;

import graphql.schema.GraphQLSchema;
import org.junit.jupiter.api.Disabled;

@Disabled
public abstract class DdlTestBase extends GraphQlTestBase {

  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService);
  }
}
