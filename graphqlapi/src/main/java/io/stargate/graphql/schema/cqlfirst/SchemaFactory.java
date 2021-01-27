/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.cqlfirst;

import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.cqlfirst.ddl.DdlSchemaBuilder;
import io.stargate.graphql.schema.cqlfirst.dml.DmlSchemaBuilder;

/** Single entry point to obtain GraphQL schemas. */
public class SchemaFactory {

  /**
   * Builds the GraphQL schema to query and modify data for a particular CQL keyspace.
   *
   * <p>This is the API exposed at {@code /graphql/<keyspaceName>}.
   */
  public static GraphQLSchema newDmlSchema(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      Keyspace keyspace,
      DataStoreFactory dataStoreFactory) {
    return new DmlSchemaBuilder(
            authenticationService, authorizationService, keyspace, dataStoreFactory)
        .build();
  }

  /**
   * Builds the GraphQL schema to manipulate the Cassandra data model, in other words create, remove
   * or alter keyspaces, tables, etc.
   *
   * <p>This is the API exposed at {@code /graphql-schema}.
   */
  public static GraphQLSchema newDdlSchema(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    return new DdlSchemaBuilder(authenticationService, authorizationService, dataStoreFactory)
        .build();
  }
}
