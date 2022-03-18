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
package io.stargate.sgv2.graphql.schema.cqlfirst;

import graphql.schema.GraphQLSchema;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlSchemaBuilder;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlSchemaBuilder;

/** Single entry point to obtain GraphQL schemas. */
public class SchemaFactory {

  /**
   * Builds the GraphQL schema to query and modify data for a particular CQL keyspace.
   *
   * <p>This is the API exposed at {@code /graphql/<keyspaceName>}.
   */
  public static GraphQLSchema newDmlSchema(CqlKeyspaceDescribe describe, String keyspaceName) {
    return new DmlSchemaBuilder(describe, keyspaceName).build();
  }

  /**
   * Builds the GraphQL schema to manipulate the Cassandra data model, in other words create, remove
   * or alter keyspaces, tables, etc.
   *
   * <p>This is the API exposed at {@code /graphql-schema}.
   */
  public static GraphQLSchema newDdlSchema() {
    return new DdlSchemaBuilder().build();
  }
}
