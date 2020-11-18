package io.stargate.graphql.schema.fetchers.ddl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import graphql.schema.GraphQLSchema;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.GraphQlTestBase;
import io.stargate.graphql.schema.SchemaFactory;

public abstract class KeyspaceFetcherTestBase extends GraphQlTestBase {

  @Override
  protected Schema createCqlSchema() {
    Schema schema = mock(Schema.class);
    Keyspace keyspace = getKeyspace();
    when(schema.keyspace(keyspace.name())).thenReturn(keyspace);
    return schema;
  }

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService, authorizationService);
  }

  public abstract Keyspace getKeyspace();
}
