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
package io.stargate.graphql.schema.cqlfirst.ddl;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.FieldCoordinates.coordinates;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLCodeRegistry.newCodeRegistry;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

import graphql.Scalars;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.AllKeyspacesFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.AlterTableAddFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.AlterTableDropFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.CreateIndexFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.CreateKeyspaceFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.CreateTableFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.CreateTypeFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.DropIndexFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.DropKeyspaceFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.DropTableFetcher;
import io.stargate.graphql.schema.cqlfirst.ddl.fetchers.SingleKeyspaceFetcher;

/** @see #INSTANCE */
public class DdlSchema {

  private static final GraphQLEnumType BASIC_TYPE =
      newEnum()
          .name("BasicType")
          .description(
              "The basic name of CQL type.\n"
                  + "For primitive types, this is enough information to define the whole type.\n"
                  + "For complex types (collections, custom, UDTs and tuples), an additional "
                  + "`DataTypeInfo` will be required.")
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
          .build();

  private static final GraphQLEnumType INDEX_KIND =
      newEnum()
          .name("IndexKind")
          .value("KEYS")
          .value("VALUES")
          .value("ENTRIES")
          .value("FULL")
          .build();

  private static final GraphQLEnumType COLUMN_KIND =
      newEnum()
          .name("ColumnKind")
          .value("COMPACT")
          .value("UNKNOWN")
          .value("PARTITION")
          .value("CLUSTERING")
          .value("REGULAR")
          .value("STATIC")
          .build();

  private static final GraphQLObjectType DATA_TYPE_INFO =
      newObject()
          .name("DataTypeInfo")
          .description("Additional information that defines a CQL type.")
          .field(
              newFieldDefinition()
                  .name("name")
                  .description(
                      "The name of the type, if it is a UDT.\n"
                          + "For other types, this is ignored.")
                  .type(GraphQLString))
          .field(
              newFieldDefinition()
                  .name("subTypes")
                  .description(
                      "The element types, if this is a collection type (list, set, map, tuple).\n"
                          + "For other types, this is ignored.")
                  .type(list(typeRef("DataType"))))
          .field(newFieldDefinition().name("frozen").type(GraphQLBoolean))
          .build();

  private static final GraphQLInputObjectType DATA_TYPE_INFO_INPUT =
      newInputObject()
          .name("DataTypeInfoInput")
          .description("Additional information that defines a CQL type.")
          .field(
              newInputObjectField()
                  .name("name")
                  .description(
                      "The name of the type, if it is a UDT.\n"
                          + "For other types, this is ignored.")
                  .type(GraphQLString))
          .field(
              newInputObjectField()
                  .name("subTypes")
                  .description(
                      "The element types, if this is a collection type (list, set, map, tuple).\n"
                          + "For other types, this is ignored.")
                  .type(list(new GraphQLTypeReference("DataTypeInput"))))
          .field(newInputObjectField().name("frozen").type(GraphQLBoolean).build())
          .build();

  private static final GraphQLObjectType DATA_TYPE =
      newObject()
          .name("DataType")
          .description("The type of a CQL column.")
          .field(newFieldDefinition().name("basic").type(nonNull(BASIC_TYPE)))
          .field(newFieldDefinition().name("info").type(DATA_TYPE_INFO))
          .build();

  private static final GraphQLInputObjectType DATA_TYPE_INPUT =
      newInputObject()
          .name("DataTypeInput")
          .description("The type of a CQL column.")
          .field(newInputObjectField().name("info").type(DATA_TYPE_INFO_INPUT))
          .field(newInputObjectField().name("basic").type(nonNull(BASIC_TYPE)))
          .build();

  private static final GraphQLObjectType FIELD =
      newObject()
          .name("Field")
          .field(newFieldDefinition().name("name").type(nonNull(GraphQLString)))
          .field(newFieldDefinition().name("type").type(nonNull(DATA_TYPE)))
          .build();

  private static final GraphQLObjectType COLUMN =
      newObject()
          .name("Column")
          .field(newFieldDefinition().name("kind").type(COLUMN_KIND))
          .field(newFieldDefinition().name("name").type(nonNull(GraphQLString)))
          .field(newFieldDefinition().name("type").type(nonNull(DATA_TYPE)))
          .build();

  private static final GraphQLInputObjectType COLUMN_INPUT =
      newInputObject()
          .name("ColumnInput")
          .description("The definition of a CQL column.")
          .field(newInputObjectField().name("name").type(nonNull(GraphQLString)))
          .field(newInputObjectField().name("type").type(nonNull(DATA_TYPE_INPUT)))
          .build();

  private static final GraphQLInputObjectType CLUSTERING_KEY_INPUT =
      newInputObject()
          .name("ClusteringKeyInput")
          .field(newInputObjectField().name("name").type(nonNull(GraphQLString)))
          .field(newInputObjectField().name("type").type(nonNull(DATA_TYPE_INPUT)))
          .field(newInputObjectField().name("order").type(GraphQLString))
          .build();

  private static final GraphQLObjectType DATA_CENTER =
      newObject()
          .name("DataCenter")
          .description("The DC-level replication options of a keyspace.")
          .field(
              newFieldDefinition()
                  .name("name")
                  .description("The name of the datacenter.")
                  .type(nonNull(GraphQLString)))
          .field(
              newFieldDefinition()
                  .name("replicas")
                  .description("The replication factor for this datacenter.")
                  .type(nonNull(Scalars.GraphQLInt)))
          .build();

  private static final GraphQLInputObjectType DATA_CENTER_INPUT =
      newInputObject()
          .name("DataCenterInput")
          .description("The DC-level replication options of a keyspace.")
          .field(
              newInputObjectField()
                  .name("name")
                  .description("The name of the datacenter.")
                  .type(nonNull(GraphQLString)))
          .field(
              newInputObjectField()
                  .name("replicas")
                  .description("The replication factor for this datacenter.")
                  .type(Scalars.GraphQLInt))
          .build();

  private static final GraphQLObjectType TABLE =
      newObject()
          .name("Table")
          .field(newFieldDefinition().name("columns").type(list(COLUMN)))
          .field(newFieldDefinition().name("name").type(nonNull(GraphQLString)))
          .build();

  private static final GraphQLObjectType UDT =
      newObject()
          .name("Type")
          .field(newFieldDefinition().name("fields").type(list(FIELD)))
          .field(newFieldDefinition().name("name").type(nonNull(GraphQLString)))
          .build();

  private static final GraphQLObjectType KEYSPACE =
      newObject()
          .name("Keyspace")
          .field(newFieldDefinition().name("dcs").type(list(DATA_CENTER)))
          .field(newFieldDefinition().name("name").type(nonNull(GraphQLString)))
          .field(
              newFieldDefinition()
                  .name("table")
                  .argument(newArgument().name("name").type(nonNull(GraphQLString)).build())
                  .type(TABLE))
          .field(newFieldDefinition().name("tables").type(list(TABLE)))
          .field(
              newFieldDefinition()
                  .name("type")
                  .argument(newArgument().name("name").type(nonNull(GraphQLString)).build())
                  .type(UDT))
          .field(newFieldDefinition().name("types").type(list(UDT)))
          .build();

  private static final GraphQLFieldDefinition CREATE_TABLE =
      newFieldDefinition()
          .name("createTable")
          .description("Creates a CQL table.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("tableName").type(nonNull(GraphQLString)))
          .argument(
              newArgument()
                  .description("The partition key columns.")
                  .name("partitionKeys")
                  .type(nonNull(list(COLUMN_INPUT))))
          .argument(
              newArgument()
                  .description("The clustering key columns.")
                  .name("clusteringKeys")
                  .type(list(CLUSTERING_KEY_INPUT)))
          .argument(
              newArgument()
                  .description("The regular columns.")
                  .name("values")
                  .type(list(COLUMN_INPUT)))
          .argument(
              newArgument()
                  .name("ifNotExists")
                  .description(
                      "What to do when the table already exists: "
                          + "`true` => mark the operation as successful but keep the existing table, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition DROP_TABLE =
      newFieldDefinition()
          .name("dropTable")
          .description("Drops a CQL table.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("tableName").type(nonNull(GraphQLString)))
          .argument(
              newArgument()
                  .name("ifExists")
                  .description(
                      "What to do when the table does not exist: "
                          + "`true` => mark the operation as successful but don't delete anything, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition ALTER_TABLE_ADD =
      newFieldDefinition()
          .name("alterTableAdd")
          .description("Adds one or more columns to a CQL table.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("tableName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("toAdd").type(nonNull(list(COLUMN_INPUT))))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition ALTER_TABLE_DROP =
      newFieldDefinition()
          .name("alterTableDrop")
          .description("Drops one or more columns from a CQL table.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("tableName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("toDrop").type(nonNull(list(GraphQLString))))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition CREATE_TYPE =
      newFieldDefinition()
          .name("createType")
          .description("Creates a CQL User-Defined Type (UDT).")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("typeName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("fields").type(nonNull(list(COLUMN_INPUT))))
          .argument(
              newArgument()
                  .name("ifNotExists")
                  .description(
                      "What to do when the type already exists: "
                          + "`true` => mark the operation as successful but keep the existing type, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition DROP_TYPE =
      newFieldDefinition()
          .name("dropType")
          .description("Drops a CQL User-Defined Type (UDT).")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("typeName").type(nonNull(GraphQLString)))
          .argument(
              newArgument()
                  .name("ifExists")
                  .description(
                      "What to do when the type does not exist: "
                          + "`true` => mark the operation as successful but don't delete anything, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition CREATE_INDEX =
      newFieldDefinition()
          .name("createIndex")
          .description("Creates a CQL index on a column.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("tableName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("columnName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("indexName").type(GraphQLString))
          .argument(
              newArgument()
                  .name("indexType")
                  .description(
                      "Adds a custom index type that can be identified by name (e.g., StorageAttachedIndex), "
                          + "or class name (e.g., org.apache.cassandra.index.sasi.SASIIndex) ")
                  .type(GraphQLString))
          .argument(
              newArgument()
                  .name("ifNotExists")
                  .description(
                      "What to do when the index already exists: "
                          + "`true` => mark the operation as successful but keep the existing index, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .argument(
              newArgument()
                  .name("indexKind")
                  .description(
                      "KEYS (indexes keys of a map),"
                          + " ENTRIES (index entries of a map),"
                          + " VALUES (index values of a collection),"
                          + " FULL (full index of a frozen collection)")
                  .type(INDEX_KIND))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition DROP_INDEX =
      newFieldDefinition()
          .name("dropIndex")
          .description("Drops a CQL index.")
          .argument(newArgument().name("keyspaceName").type(nonNull(GraphQLString)))
          .argument(newArgument().name("indexName").type(nonNull(GraphQLString)))
          .argument(
              newArgument()
                  .name("ifExists")
                  .description(
                      "What to do when the index does not exist: "
                          + "`true` => mark the operation as successful but don't delete anything, "
                          + "`false` => throw an error.")
                  .type(GraphQLBoolean)
                  .defaultValue(false))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition CREATE_KEYSPACE =
      newFieldDefinition()
          .name("createKeyspace")
          .description("Creates a new CQL keyspace")
          .argument(
              newArgument()
                  .name("name")
                  .type(nonNull(GraphQLString))
                  .description("The name of the keyspace"))
          .argument(
              newArgument()
                  .name("ifNotExists")
                  .type(GraphQLBoolean)
                  .defaultValue(false)
                  .description(
                      "What to do when the keyspace already exists: "
                          + "`true` => mark the operation as successful but keep the existing keyspace, "
                          + "`false` => throw an error."))
          .argument(
              newArgument()
                  .name("replicas")
                  .type(Scalars.GraphQLInt)
                  .description(
                      "Enables SimpleStrategy replication with the given replication factor. "
                          + "You must specify either this or 'datacenters', but not both."))
          .argument(
              newArgument()
                  .name("datacenters")
                  .type(list(DATA_CENTER_INPUT))
                  .description(
                      "Enables NetworkTopologyStrategy with the given replication factors per DC. "
                          + "(at least one DC must be specified)."
                          + "You must specify either this or 'replicas', but not both.")
                  .build())
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLFieldDefinition DROP_KEYSPACE =
      newFieldDefinition()
          .name("dropKeyspace")
          .description("Drops a CQL keyspace")
          .argument(
              newArgument()
                  .name("name")
                  .type(nonNull(GraphQLString))
                  .description("The name of the keyspace"))
          .argument(
              newArgument()
                  .name("ifExists")
                  .type(GraphQLBoolean)
                  .defaultValue(false)
                  .description(
                      "What to do when the keyspace does not exist: "
                          + "`true` => mark the operation as successful but don't delete anything, "
                          + "`false` => throw an error."))
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLObjectType MUTATION =
      newObject()
          .name("Mutation")
          .field(CREATE_TABLE)
          .field(ALTER_TABLE_ADD)
          .field(ALTER_TABLE_DROP)
          .field(DROP_TABLE)
          .field(CREATE_TYPE)
          .field(DROP_TYPE)
          .field(CREATE_INDEX)
          .field(DROP_INDEX)
          .field(CREATE_KEYSPACE)
          .field(DROP_KEYSPACE)
          .build();

  private static final GraphQLFieldDefinition KEYSPACE_BY_NAME_QUERY =
      newFieldDefinition()
          .name("keyspace")
          .argument(newArgument().name("name").type(nonNull(GraphQLString)))
          .type(KEYSPACE)
          .build();

  private static final GraphQLFieldDefinition KEYSPACES_QUERY =
      newFieldDefinition().name("keyspaces").type(list(KEYSPACE)).build();

  private static final GraphQLObjectType QUERY =
      newObject().name("Query").field(KEYSPACE_BY_NAME_QUERY).field(KEYSPACES_QUERY).build();

  public static final GraphQLSchema INSTANCE =
      new GraphQLSchema.Builder()
          .mutation(MUTATION)
          .query(QUERY)
          .codeRegistry(
              newCodeRegistry()
                  .dataFetcher(coordinates(MUTATION, CREATE_TABLE), new CreateTableFetcher())
                  .dataFetcher(coordinates(MUTATION, ALTER_TABLE_ADD), new AlterTableAddFetcher())
                  .dataFetcher(coordinates(MUTATION, ALTER_TABLE_DROP), new AlterTableDropFetcher())
                  .dataFetcher(coordinates(MUTATION, DROP_TABLE), new DropTableFetcher())
                  .dataFetcher(coordinates(MUTATION, CREATE_TYPE), new CreateTypeFetcher())
                  .dataFetcher(coordinates(MUTATION, DROP_TYPE), new DropTableFetcher())
                  .dataFetcher(coordinates(MUTATION, CREATE_INDEX), new CreateIndexFetcher())
                  .dataFetcher(coordinates(MUTATION, DROP_INDEX), new DropIndexFetcher())
                  .dataFetcher(coordinates(MUTATION, CREATE_KEYSPACE), new CreateKeyspaceFetcher())
                  .dataFetcher(coordinates(MUTATION, DROP_KEYSPACE), new DropKeyspaceFetcher())
                  .dataFetcher(
                      coordinates(QUERY, KEYSPACE_BY_NAME_QUERY), new SingleKeyspaceFetcher())
                  .dataFetcher(coordinates(QUERY, KEYSPACES_QUERY), new AllKeyspacesFetcher())
                  .build())
          .build();
}
