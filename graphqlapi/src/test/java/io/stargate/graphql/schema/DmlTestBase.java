package io.stargate.graphql.schema;

import graphql.schema.GraphQLSchema;
import io.stargate.db.schema.Keyspace;
import org.junit.jupiter.api.Disabled;

@Disabled
public abstract class DmlTestBase extends GraphQlTestBase {

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDmlSchema(persistence, authenticationService, getKeyspace());
  }

  public abstract Keyspace getKeyspace();
}
