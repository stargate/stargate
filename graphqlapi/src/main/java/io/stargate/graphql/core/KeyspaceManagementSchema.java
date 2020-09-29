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
package io.stargate.graphql.core;

import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.graphql.fetchers.AlterTableAddFetcher;
import io.stargate.graphql.fetchers.AlterTableDropFetcher;
import io.stargate.graphql.fetchers.CreateKeyspaceFetcher;
import io.stargate.graphql.fetchers.CreateTableDataFetcher;
import io.stargate.graphql.fetchers.DropTableFetcher;
import io.stargate.graphql.fetchers.KeyspaceFetcher;
import io.stargate.graphql.fetchers.SchemaDataFetcherFactory;
import java.util.HashMap;

public class KeyspaceManagementSchema {
  private final HashMap<String, GraphQLType> objects;
  private final Persistence persistence;
  private AuthenticationService authenticationService;
  private SchemaDataFetcherFactory schemaDataFetcherFactory;

  public KeyspaceManagementSchema(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.objects = new HashMap<>();
    this.authenticationService = authenticationService;
    this.schemaDataFetcherFactory =
        new SchemaDataFetcherFactory(persistence, authenticationService);
  }

  public GraphQLSchema.Builder build() {
    GraphQLSchema.Builder builder = new GraphQLSchema.Builder();
    builder.mutation(
        buildMutation(
            buildCreateTable(),
            buildAlterTableAdd(),
            buildAlterTableDrop(),
            buildDrop(),
            buildCreateKeyspace()));
    builder.query(buildQuery(buildKeyspaceByName(), buildKeyspaces()));
    return builder;
  }

  private GraphQLFieldDefinition buildAlterTableAdd() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("alterTableAdd")
        .argument(
            GraphQLArgument.newArgument().name("keyspaceName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("tableName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("toAdd").type(nonNull(list(buildColumnInput()))))
        .type(Scalars.GraphQLBoolean)
        .dataFetcher(
            schemaDataFetcherFactory.createSchemaFetcher(AlterTableAddFetcher.class.getName()))
        .build();
  }

  private GraphQLFieldDefinition buildAlterTableDrop() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("alterTableDrop")
        .argument(
            GraphQLArgument.newArgument().name("keyspaceName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("tableName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("toDrop").type(nonNull(list(Scalars.GraphQLString))))
        .type(Scalars.GraphQLBoolean)
        .dataFetcher(
            schemaDataFetcherFactory.createSchemaFetcher(AlterTableDropFetcher.class.getName()))
        .build();
  }

  private GraphQLFieldDefinition buildDrop() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("dropTable")
        .argument(
            GraphQLArgument.newArgument().name("keyspaceName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("tableName").type(nonNull(Scalars.GraphQLString)))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .type(Scalars.GraphQLBoolean)
        .dataFetcher(schemaDataFetcherFactory.createSchemaFetcher(DropTableFetcher.class.getName()))
        .build();
  }

  private GraphQLFieldDefinition buildCreateKeyspace() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("createKeyspace")
        .description("Creates a new CQL keyspace")
        .argument(
            GraphQLArgument.newArgument()
                .name("name")
                .type(nonNull(Scalars.GraphQLString))
                .description("The name of the keyspace"))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifNotExists")
                .type(Scalars.GraphQLBoolean)
                .description(
                    "Whether the operation will succeed if the keyspace already exists. "
                        + "Defaults to false if absent."))
        .argument(
            GraphQLArgument.newArgument()
                .name("replicas")
                .type(Scalars.GraphQLInt)
                .description(
                    "Enables SimpleStrategy replication with the given replication factor. "
                        + "You must specify either this or 'datacenters', but not both."))
        .argument(
            GraphQLArgument.newArgument()
                .name("datacenters")
                .type(list(buildDataCenterInput()))
                .description(
                    "Enables NetworkTopologyStrategy with the given replication factors per DC. "
                        + "(at least one DC must be specified)."
                        + "You must specify either this or 'replicas', but not both.")
                .build())
        .type(Scalars.GraphQLBoolean)
        .dataFetcher(
            schemaDataFetcherFactory.createSchemaFetcher(CreateKeyspaceFetcher.class.getName()))
        .build();
  }

  private GraphQLObjectType buildQuery(GraphQLFieldDefinition... defs) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");
    for (GraphQLFieldDefinition def : defs) {
      builder.field(def);
    }
    return builder.build();
  }

  private GraphQLFieldDefinition buildKeyspaceByName() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("keyspace")
        .argument(GraphQLArgument.newArgument().name("name").type(nonNull(Scalars.GraphQLString)))
        .type(buildKeyspace())
        .dataFetcher(
            new KeyspaceFetcher(persistence, authenticationService).new KeyspaceByNameFetcher())
        .build();
  }

  private GraphQLObjectType buildKeyspace() {
    return register(
        GraphQLObjectType.newObject()
            .name("Keyspace")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("dcs")
                    .type(list(buildDataCenter())))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("table")
                    .argument(
                        GraphQLArgument.newArgument()
                            .name("name")
                            .type(nonNull(Scalars.GraphQLString))
                            .build())
                    .type(buildTableType()))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("tables")
                    .type(list(buildTableType())))
            .build());
  }

  private GraphQLObjectType buildTableType() {
    return register(
        GraphQLObjectType.newObject()
            .name("Table")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("columns")
                    .type(list(buildColumnType())))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .build());
  }

  private GraphQLType buildColumnType() {
    return register(
        GraphQLObjectType.newObject()
            .name("Column")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("kind")
                    .type(nonNull(buildColumnKind())))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("type")
                    .type(nonNull(buildDataType())))
            .build());
  }

  private GraphQLType buildDataType() {
    return register(
        GraphQLObjectType.newObject()
            .name("DataType")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("basic")
                    .type(nonNull(buildBasicType())))
            .field(
                GraphQLFieldDefinition.newFieldDefinition().name("info").type(buildDataTypeInfo()))
            .build());
  }

  private GraphQLOutputType buildDataTypeInfo() {
    return register(
        GraphQLObjectType.newObject()
            .name("DataTypeInfo")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(Scalars.GraphQLString))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("subTypes")
                    .type(list(new GraphQLTypeReference("DataType"))))
            .build());
  }

  private GraphQLType buildColumnKind() {
    return register(
        GraphQLEnumType.newEnum()
            .name("ColumnKind")
            .value("COMPACT")
            .value("UNKNOWN")
            .value("PARTITION")
            .value("CLUSTERING")
            .value("REGULAR")
            .value("STATIC")
            .build());
  }

  private GraphQLObjectType buildDataCenter() {
    return register(
        GraphQLObjectType.newObject()
            .name("DataCenter")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("replicas")
                    .type(nonNull(Scalars.GraphQLInt)))
            .build());
  }

  private GraphQLInputObjectType buildDataCenterInput() {
    return register(
        GraphQLInputObjectType.newInputObject()
            .name("DataCenterInput")
            .description("The DC-level replication options passed to 'createKeyspace.datacenters'.")
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("name")
                    .description("The name of the datacenter.")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("replicas")
                    .description("The replication factor for this datacenter.")
                    .type(Scalars.GraphQLInt))
            .build());
  }

  private GraphQLFieldDefinition buildKeyspaces() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("keyspaces")
        .type(list(buildKeyspace()))
        .dataFetcher(new KeyspaceFetcher(persistence, authenticationService).new KeyspacesFetcher())
        .build();
  }

  private GraphQLObjectType buildMutation(GraphQLFieldDefinition... defs) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
    for (GraphQLFieldDefinition def : defs) {
      builder.field(def);
    }
    return builder.build();
  }

  private GraphQLFieldDefinition buildCreateTable() {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("createTable")
        .argument(
            GraphQLArgument.newArgument().name("keyspaceName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument().name("tableName").type(nonNull(Scalars.GraphQLString)))
        .argument(
            GraphQLArgument.newArgument()
                .name("partitionKeys")
                .type(nonNull(list(buildColumnInput()))))
        .argument(
            GraphQLArgument.newArgument()
                .name("clusteringKeys")
                .type(list(buildClusteringKeyInput())))
        .argument(GraphQLArgument.newArgument().name("values").type(list(buildColumnInput())))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .type(Scalars.GraphQLBoolean)
        .dataFetcher(
            schemaDataFetcherFactory.createSchemaFetcher(CreateTableDataFetcher.class.getName()))
        .build();
  }

  private GraphQLInputObjectType buildClusteringKeyInput() {
    return register(
        GraphQLInputObjectType.newInputObject()
            .name("ClusteringKeyInput")
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("type")
                    .type(nonNull(buildDataTypeInput())))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("order")
                    .type(Scalars.GraphQLString))
            .build());
  }

  private GraphQLInputObjectType buildColumnInput() {
    return register(
        GraphQLInputObjectType.newInputObject()
            .name("ColumnInput")
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("name")
                    .type(nonNull(Scalars.GraphQLString)))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("type")
                    .type(nonNull(buildDataTypeInput())))
            .build());
  }

  private GraphQLInputObjectType buildDataTypeInput() {
    return register(
        GraphQLInputObjectType.newInputObject()
            .name("DataTypeInput")
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("info")
                    .type(buildDataTypeInfoInput()))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("basic")
                    .type(nonNull(buildBasicType())))
            .build());
  }

  private GraphQLInputType buildBasicType() {
    return register(
        GraphQLEnumType.newEnum()
            .name("BasicType")
            .value("CUSTOM")
            .value("INT")
            .value("TIMEUUID")
            .value("TIMESTAMP")
            .value("UDT")
            .value("BIGINT")
            .value("TIME")
            .value("DURATION")
            .value("VARINT")
            .value("UUID")
            .value("BOOLEAN")
            .value("TINYINT")
            .value("SMALLINT")
            .value("INET")
            .value("ASCII")
            .value("DECIMAL")
            .value("BLOB")
            .value("LIST")
            .value("MAP")
            .value("VARCHAR")
            .value("TUPLE")
            .value("DOUBLE")
            .value("COUNTER")
            .value("DATE")
            .value("TEXT")
            .value("FLOAT")
            .value("SET")
            .build());
  }

  private GraphQLInputObjectType buildDataTypeInfoInput() {
    return register(
        GraphQLInputObjectType.newInputObject()
            .name("DataTypeInfoInput")
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("subTypes")
                    .type(list(new GraphQLTypeReference("DataTypeInput"))))
            .field(
                GraphQLInputObjectField.newInputObjectField()
                    .name("name")
                    .type(Scalars.GraphQLString))
            .build());
  }

  private <T> T register(GraphQLType object) {
    String name = getName(object);
    if (name == null) {
      throw new RuntimeException("Schema object has no name" + object);
    }

    Object o = objects.get(name);
    if (o == null) {
      objects.put(name, object);
      return (T) object;
    }
    return (T) o;
  }

  private String getName(GraphQLType object) {
    if (object instanceof GraphQLInputObjectType) {
      return ((GraphQLInputObjectType) object).getName();
    } else if (object instanceof GraphQLObjectType) {
      return ((GraphQLObjectType) object).getName();
    } else if (object instanceof GraphQLEnumType) {
      return ((GraphQLEnumType) object).getName();
    }
    return null;
  }
}
