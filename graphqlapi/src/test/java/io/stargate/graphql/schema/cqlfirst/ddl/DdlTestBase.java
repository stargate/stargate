package io.stargate.graphql.schema.cqlfirst.ddl;

import graphql.schema.GraphQLSchema;
import io.stargate.graphql.schema.GraphQlTestBase;
import io.stargate.graphql.schema.cqlfirst.SchemaFactory;

public abstract class DdlTestBase extends GraphQlTestBase {

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema();
  }
}
