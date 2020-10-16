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
package io.stargate.graphql.schema;

import static graphql.Scalars.GraphQLString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.fetchers.dml.DeleteMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.InsertMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.QueryFetcher;
import io.stargate.graphql.schema.fetchers.dml.UpdateMutationFetcher;
import io.stargate.graphql.schema.types.GqlMapBuilder;
import io.stargate.graphql.util.CaseUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DmlSchemaBuilder {
  private static final Logger log = LoggerFactory.getLogger(DmlSchemaBuilder.class);

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final Map<Column.ColumnType, GraphQLInputObjectType> filterInputTypes;
  private final Map<Table, GraphQLOutputType> entityResultMap = new HashMap<>();
  private final Map<String, GraphQLType> typeCache = new HashMap<>();
  private final NameMapping nameMapping;
  private final Keyspace keyspace;

  private GraphQLInputType mutationOptions =
      GraphQLInputObjectType.newInputObject()
          .name("MutationOptions")
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("consistency")
                  .type(
                      GraphQLEnumType.newEnum()
                          .name("MutationConsistency")
                          .value("LOCAL_ONE")
                          .value("LOCAL_QUORUM")
                          .value("ALL")
                          .build())
                  .build())
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("serialConsistency")
                  .type(
                      GraphQLEnumType.newEnum()
                          .name("SerialConsistency")
                          .value("SERIAL")
                          .value("LOCAL_SERIAL")
                          .build())
                  .build())
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("ttl")
                  .type(Scalars.GraphQLInt)
                  .defaultValue(-1)
                  .build())
          .build();

  DmlSchemaBuilder(
      Persistence persistence, AuthenticationService authenticationService, Keyspace keyspace) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    this.keyspace = keyspace;

    this.nameMapping = new NameMapping(keyspace.tables(), keyspace.userDefinedTypes());
    this.filterInputTypes = buildFilterInputTypes();
  }

  GraphQLSchema build() {
    GraphQLSchema.Builder builder = new GraphQLSchema.Builder();

    for (UserDefinedType udt : keyspace.userDefinedTypes()) {
      builder.additionalTypes(buildTypesForUdt(udt));
    }

    List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();

    // Tables must be iterated one at a time. If a table is unfulfillable, it is skipped
    for (Table table : keyspace.tables()) {
      Set<GraphQLType> additionalTypes;
      List<GraphQLFieldDefinition> tableQueryField;
      List<GraphQLFieldDefinition> tableMutationFields;

      try {
        additionalTypes = buildTypesForTable(table);
        tableQueryField = buildQuery(table);
        tableMutationFields = buildMutations(table);
      } catch (Exception e) {
        log.warn("Skipping table " + table.name());
        continue;
      }

      builder.additionalTypes(additionalTypes);
      queryFields.addAll(tableQueryField);
      mutationFields.addAll(tableMutationFields);
    }

    if (queryFields.isEmpty()) {
      GraphQLFieldDefinition emptyQueryField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("__keyspaceEmptyQuery")
              .description("Placeholder query that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      queryFields.add(emptyQueryField);
    }

    if (mutationFields.isEmpty()) {
      GraphQLFieldDefinition emptyMutationField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("__keyspaceEmptyMutation")
              .description("Placeholder mutation that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      mutationFields.add(emptyMutationField);
    }

    builder.additionalType(buildQueryOptionsInputType());
    builder.query(buildQueries(queryFields));
    builder.mutation(buildMutationRoot(mutationFields));
    return builder.build();
  }

  private GraphQLObjectType buildMutationRoot(List<GraphQLFieldDefinition> mutationFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
    for (GraphQLFieldDefinition mutation : mutationFields) {
      builder.field(mutation);
    }

    return builder.build();
  }

  private GraphQLObjectType buildQueries(List<GraphQLFieldDefinition> queryFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");

    for (GraphQLFieldDefinition fieldDefinition : queryFields) {
      builder.field(fieldDefinition);
    }

    return builder.build();
  }

  private List<GraphQLFieldDefinition> buildQuery(Table table) {
    GraphQLFieldDefinition query =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(CaseUtil.toLowerCamel(table.name()))
            .argument(
                GraphQLArgument.newArgument()
                    .name("value")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "Input")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(
                                nameMapping.getEntityNames().get(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(table, nameMapping, persistence, authenticationService))
            .build();

    GraphQLFieldDefinition filterQuery =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(CaseUtil.toLowerCamel(table.name()) + "Filter")
            .deprecate("No longer supported. Use root type instead.")
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(
                                nameMapping.getEntityNames().get(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(table, nameMapping, persistence, authenticationService))
            .build();

    return ImmutableList.of(query, filterQuery);
  }

  private List<GraphQLFieldDefinition> buildMutations(Table table) {
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();
    mutationFields.add(buildDelete(table));
    mutationFields.add(buildInsert(table));
    mutationFields.add(buildUpdate(table));

    return mutationFields;
  }

  private Set<GraphQLType> buildTypesForTable(Table table) {
    Set<GraphQLType> additionalTypes = new HashSet<>();

    additionalTypes.add(buildType(table));
    additionalTypes.add(buildInputType(table));
    additionalTypes.add(buildOrderType(table));
    additionalTypes.add(buildMutationResult(table));
    additionalTypes.add(buildFilterInput(table));
    return additionalTypes;
  }

  private GraphQLType buildFilterInput(Table table) {
    return GraphQLInputObjectType.newInputObject()
        .name(nameMapping.getEntityNames().get(table) + "FilterInput")
        .fields(buildFilterInputFields(table))
        .build();
  }

  private GraphQLFieldDefinition buildUpdate(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("update" + nameMapping.getEntityNames().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(
                    new GraphQLTypeReference(
                        nameMapping.getEntityNames().get(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityNames().get(table) + "MutationResult"))
        .dataFetcher(
            new UpdateMutationFetcher(table, nameMapping, persistence, authenticationService))
        .build();
  }

  private GraphQLFieldDefinition buildInsert(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("insert" + nameMapping.getEntityNames().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityNames().get(table) + "MutationResult"))
        .dataFetcher(
            new InsertMutationFetcher(table, nameMapping, persistence, authenticationService))
        .build();
  }

  private GraphQLFieldDefinition buildDelete(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("delete" + nameMapping.getEntityNames().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityNames().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(
                    new GraphQLTypeReference(
                        nameMapping.getEntityNames().get(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityNames().get(table) + "MutationResult"))
        .dataFetcher(
            new DeleteMutationFetcher(table, nameMapping, persistence, authenticationService))
        .build();
  }

  private Map<Column.ColumnType, GraphQLInputObjectType> buildFilterInputTypes() {
    GraphQLInputObjectType stringFilterInput = filterInputType(Scalars.GraphQLString);
    GraphQLInputObjectType intFilterInput = filterInputType(Scalars.GraphQLInt);
    GraphQLInputObjectType floatFilterInput = filterInputType(Scalars.GraphQLFloat);
    GraphQLInputObjectType uuidFilterInput = filterInputType(CustomScalar.UUID.getGraphQLScalar());
    GraphQLInputObjectType timestampFilterInput =
        filterInputType(CustomScalar.TIMESTAMP.getGraphQLScalar());
    GraphQLInputObjectType timeUUIDFilterInput =
        filterInputType(CustomScalar.TIMEUUID.getGraphQLScalar());
    GraphQLInputObjectType inetFilterInput = filterInputType(CustomScalar.INET.getGraphQLScalar());
    GraphQLInputObjectType bigIntFilterInput =
        filterInputType(CustomScalar.BIGINT.getGraphQLScalar());
    GraphQLInputObjectType decimalFilterInput =
        filterInputType(CustomScalar.DECIMAL.getGraphQLScalar());
    GraphQLInputObjectType varintFilterInput =
        filterInputType(CustomScalar.VARINT.getGraphQLScalar());
    GraphQLInputObjectType blobFilterInput = filterInputType(CustomScalar.BLOB.getGraphQLScalar());

    return ImmutableMap.<Column.ColumnType, GraphQLInputObjectType>builder()
        .put(Column.Type.Int, intFilterInput)
        .put(Column.Type.Smallint, intFilterInput)
        .put(Column.Type.Tinyint, intFilterInput)
        .put(Column.Type.Text, stringFilterInput)
        .put(Column.Type.Varchar, stringFilterInput)
        .put(Column.Type.Float, floatFilterInput)
        .put(Column.Type.Double, floatFilterInput)
        .put(Column.Type.Uuid, uuidFilterInput)
        .put(Column.Type.Timestamp, timestampFilterInput)
        .put(Column.Type.Timeuuid, timeUUIDFilterInput)
        .put(Column.Type.Inet, inetFilterInput)
        .put(Column.Type.Bigint, bigIntFilterInput)
        .put(Column.Type.Decimal, decimalFilterInput)
        .put(Column.Type.Varint, varintFilterInput)
        .put(Column.Type.Blob, blobFilterInput)
        .build();
  }

  private List<GraphQLInputObjectField> buildFilterInputFields(Table table) {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    for (Column columnMetadata : table.columns()) {
      Column.ColumnType type = columnMetadata.type();
      if (type != null && type.isUserDefined()) {
        assert type instanceof UserDefinedType;
        fields.add(
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getColumnNames(table).get(columnMetadata))
                .type(
                    new GraphQLTypeReference(
                        nameMapping.getUdtNames().get(type.frozen(false)) + "FilterInput"))
                .build());
      } else if (filterInputTypes.get(type) != null) {
        fields.add(
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getColumnNames(table).get(columnMetadata))
                .type(getFilterInputTypeRef(type))
                .build());
      }
    }

    Preconditions.checkState(
        !fields.isEmpty(), "Could not generate an input type for table, skipping.");

    return fields;
  }

  private GraphQLInputType getFilterInputTypeRef(Column.ColumnType dataType) {
    return filterInputTypes.get(dataType);
  }

  private static GraphQLInputObjectType filterInputType(GraphQLScalarType type) {
    return filterInputType(type.getName() + "FilterInput", type);
  }

  private static GraphQLInputObjectType filterInputType(
      String filterInputTypeName, GraphQLInputType elementType) {
    return GraphQLInputObjectType.newInputObject()
        .name(filterInputTypeName)
        .field(GraphQLInputObjectField.newInputObjectField().name("eq").type(elementType))
        .field(GraphQLInputObjectField.newInputObjectField().name("notEq").type(elementType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gt").type(elementType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gte").type(elementType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lt").type(elementType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lte").type(elementType))
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("in")
                .type(new GraphQLList(elementType)))
        .build();
  }

  private GraphQLOutputType buildMutationResult(Table table) {
    return GraphQLObjectType.newObject()
        .name(nameMapping.getEntityNames().get(table) + "MutationResult")
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("applied")
                .type(Scalars.GraphQLBoolean))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("value")
                .type(new GraphQLTypeReference(nameMapping.getEntityNames().get(table))))
        .build();
  }

  private GraphQLType buildQueryOptionsInputType() {
    return GraphQLInputObjectType.newInputObject()
        .name("QueryOptions")
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("consistency")
                .type(
                    GraphQLEnumType.newEnum()
                        .name("QueryConsistency")
                        .value("LOCAL_ONE")
                        .value("LOCAL_QUORUM")
                        .value("ALL")
                        .value("SERIAL")
                        .value("LOCAL_SERIAL")
                        .build())
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("limit")
                .type(Scalars.GraphQLInt)
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("pageSize")
                .type(Scalars.GraphQLInt)
                .defaultValue(100)
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("pageState")
                .type(Scalars.GraphQLString)
                .build())
        .build();
  }

  private GraphQLType buildOrderType(Table table) {
    GraphQLEnumType.Builder input =
        GraphQLEnumType.newEnum().name(nameMapping.getEntityNames().get(table) + "Order");
    for (Column columnMetadata : table.columns()) {
      input.value(nameMapping.getColumnNames(table).get(columnMetadata) + "_DESC");
      input.value(nameMapping.getColumnNames(table).get(columnMetadata) + "_ASC");
    }
    return input.build();
  }

  private GraphQLType buildInputType(Table table) {
    GraphQLInputObjectType.Builder input =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getEntityNames().get(table) + "Input");
    for (Column columnMetadata : table.columns()) {
      try {
        GraphQLInputObjectField field =
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getColumnNames(table).get(columnMetadata))
                .type((GraphQLInputType) getGraphQLType(columnMetadata.type(), true))
                .build();
        input.field(field);
      } catch (Exception e) {
        log.error(
            String.format("Input type for %s could not be created", columnMetadata.name()), e);
      }
    }
    return input.build();
  }

  private GraphQLOutputType buildEntityResultOutput(Table table) {
    if (entityResultMap.containsKey(table)) {
      return entityResultMap.get(table);
    }

    GraphQLOutputType entityResultType =
        GraphQLObjectType.newObject()
            .name(nameMapping.getEntityNames().get(table) + "Result")
            .field(
                GraphQLFieldDefinition.newFieldDefinition().name("pageState").type(GraphQLString))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("values")
                    .type(
                        new GraphQLList(
                            new GraphQLNonNull(
                                new GraphQLTypeReference(
                                    nameMapping.getEntityNames().get(table))))))
            .build();

    entityResultMap.put(table, entityResultType);

    return entityResultType;
  }

  public GraphQLObjectType buildType(Table table) {
    GraphQLObjectType.Builder builder =
        GraphQLObjectType.newObject().name(nameMapping.getEntityNames().get(table));
    for (Column columnMetadata : table.columns()) {
      try {
        GraphQLFieldDefinition.Builder fieldBuilder = buildOutputField(table, columnMetadata);
        builder.field(fieldBuilder.build());
      } catch (Exception e) {
        log.error(String.format("Type for %s could not be created", columnMetadata.name()), e);
      }
    }

    return builder.build();
  }

  private GraphQLFieldDefinition.Builder buildOutputField(Table table, Column columnMetadata) {
    return new GraphQLFieldDefinition.Builder()
        .name(nameMapping.getColumnNames(table).get(columnMetadata))
        .type((GraphQLOutputType) getGraphQLType(columnMetadata.type(), false));
  }

  private Set<GraphQLType> buildTypesForUdt(UserDefinedType udt) {

    String typeNameBase = nameMapping.getUdtNames().get(udt.frozen(false));

    GraphQLObjectType.Builder outputType = GraphQLObjectType.newObject().name(typeNameBase);
    for (Column column : udt.columns()) {
      try {
        outputType.field(
            new GraphQLFieldDefinition.Builder()
                .name(nameMapping.getFieldNames(udt).get(column))
                .type((GraphQLOutputType) getGraphQLType(column.type(), false))
                .build());
      } catch (Exception e) {
        log.error(String.format("Type for %s could not be created", column.name()), e);
      }
    }

    GraphQLInputObjectType.Builder inputType =
        GraphQLInputObjectType.newInputObject().name(typeNameBase + "Input");
    for (Column column : udt.columns()) {
      try {
        inputType.field(
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getFieldNames(udt).get(column))
                .type((GraphQLInputType) getGraphQLType(column.type(), true))
                .build());
      } catch (Exception e) {
        log.error(String.format("Input type for %s could not be created", column.name()), e);
      }
    }
    GraphQLInputObjectType filterInputType =
        filterInputType(
            typeNameBase + "FilterInput", new GraphQLTypeReference(typeNameBase + "Input"));

    return ImmutableSet.of(outputType.build(), inputType.build(), filterInputType);
  }

  private GraphQLType getGraphQLType(Column.ColumnType type, boolean isInput) {
    return getGraphQLType(type, isInput, typeCache, nameMapping);
  }

  @VisibleForTesting
  static GraphQLType getGraphQLType(
      Column.ColumnType type,
      boolean isInput,
      Map<String, GraphQLType> typeCache,
      NameMapping nameMapping) {
    switch (type.rawType()) {
      case Ascii:
        return CustomScalar.ASCII.getGraphQLScalar();
      case Bigint:
        return CustomScalar.BIGINT.getGraphQLScalar();
      case Blob:
        return CustomScalar.BLOB.getGraphQLScalar();
      case Boolean:
        return Scalars.GraphQLBoolean;
      case Counter:
        return CustomScalar.COUNTER.getGraphQLScalar();
      case Decimal:
        return CustomScalar.DECIMAL.getGraphQLScalar();
      case Double:
        // GraphQL's Float is a signed double‐precision fractional value
        return Scalars.GraphQLFloat;
      case Float:
        // Use a custom scalar named "Float32"
        return CustomScalar.FLOAT.getGraphQLScalar();
      case Int:
      case Smallint:
      case Tinyint:
        return Scalars.GraphQLInt;
      case Text:
      case Varchar:
        return Scalars.GraphQLString;
      case Timestamp:
        return CustomScalar.TIMESTAMP.getGraphQLScalar();
      case Uuid:
        return CustomScalar.UUID.getGraphQLScalar();
      case Varint:
        return CustomScalar.VARINT.getGraphQLScalar();
      case Timeuuid:
        return CustomScalar.TIMEUUID.getGraphQLScalar();
      case Inet:
        return CustomScalar.INET.getGraphQLScalar();
      case Date:
        return CustomScalar.DATE.getGraphQLScalar();
      case Time:
        return CustomScalar.TIME.getGraphQLScalar();
      case List:
      case Set:
        return new GraphQLList(
            getGraphQLType(type.parameters().get(0), isInput, typeCache, nameMapping));
      case Map:
        GraphQLType keyType =
            getGraphQLType(type.parameters().get(0), isInput, typeCache, nameMapping);
        GraphQLType valueType =
            getGraphQLType(type.parameters().get(1), isInput, typeCache, nameMapping);
        return new GqlMapBuilder(keyType, valueType, isInput, typeCache).build();
      case UDT:
        UserDefinedType udt = (UserDefinedType) type.frozen(false);
        return new GraphQLTypeReference(
            nameMapping.getUdtNames().get(udt) + (isInput ? "Input" : ""));
      default:
        break;
    }

    throw new RuntimeException("Unsupported data type " + type.name());
  }
}
