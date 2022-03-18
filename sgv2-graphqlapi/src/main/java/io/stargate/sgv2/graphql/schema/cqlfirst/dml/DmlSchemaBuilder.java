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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLList.list;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.Scalars;
import graphql.introspection.Introspection;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
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
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.SchemaConstants;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.BulkInsertMutationFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.DeleteMutationFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.InsertMutationFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.QueryFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.UpdateMutationFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DmlSchemaBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DmlSchemaBuilder.class);

  private static final GraphQLInputType MUTATION_OPTIONS =
      GraphQLInputObjectType.newInputObject()
          .name("MutationOptions")
          .description("The execution options for the mutation.")
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
                  .defaultValue(CassandraFetcher.DEFAULT_CONSISTENCY.getValue().name())
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
                  .defaultValue(CassandraFetcher.DEFAULT_SERIAL_CONSISTENCY.getValue().name())
                  .build())
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("ttl")
                  .type(Scalars.GraphQLInt)
                  .build())
          .build();

  private final CqlKeyspaceDescribe cqlSchema;
  private final String keyspaceName;
  private final List<String> warnings = new ArrayList<>();
  private final FieldInputTypeCache fieldInputTypes;
  private final FieldOutputTypeCache fieldOutputTypes;
  private final FieldFilterInputTypeCache fieldFilterInputTypes;
  private final NameMapping nameMapping;
  private final Map<CqlTable, GraphQLOutputType> entityResultMap = new HashMap<>();

  /** Describes the different kind of types generated from a table */
  private enum DmlType {
    QueryOutput,
    MutationOutput,
    Input,
    FilterInput,
    Order
  }

  public DmlSchemaBuilder(CqlKeyspaceDescribe cqlSchema, String keyspaceName) {
    this.cqlSchema = cqlSchema;
    // Note that cqlSchema also contains the keyspace name, but it's the decorated one. We pass the
    // undecorated one all the way from GraphqlCache, because that's what we want to use in the
    // user-facing GraphQL schema.
    this.keyspaceName = keyspaceName;

    this.nameMapping =
        new NameMapping(cqlSchema.getTablesList(), cqlSchema.getTypesList(), warnings);
    this.fieldInputTypes = new FieldInputTypeCache(this.nameMapping, warnings);
    this.fieldOutputTypes = new FieldOutputTypeCache(this.nameMapping, warnings);
    this.fieldFilterInputTypes =
        new FieldFilterInputTypeCache(this.fieldInputTypes, this.nameMapping);
  }

  public GraphQLSchema build() {
    GraphQLSchema.Builder builder = new GraphQLSchema.Builder();

    List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();

    // Tables must be iterated one at a time. If a table is unfulfillable, it is skipped
    for (CqlTable table : cqlSchema.getTablesList()) {
      if (nameMapping.getGraphqlName(table) == null) {
        // This means there was a name clash. We already added a warning in NameMapping.
        continue;
      }

      try {
        builder.additionalTypes(buildTypesForTable(table));
        queryFields.addAll(buildQuery(table));
        mutationFields.addAll(buildMutations(table));
      } catch (Exception e) {
        warn(e, "Could not convert table %s, skipping", table.getName());
      }
    }

    addAtomicDirective(builder);
    addAsyncDirective(builder);

    builder.additionalType(buildQueryOptionsInputType());

    queryFields.add(buildWarnings());
    builder.query(buildQueries(queryFields));

    if (mutationFields.isEmpty()) {
      GraphQLFieldDefinition emptyMutationField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("keyspaceEmptyMutation")
              .description("Placeholder mutation that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      mutationFields.add(emptyMutationField);
    }
    builder.mutation(buildMutationRoot(mutationFields));

    return builder.build();
  }

  private Set<GraphQLType> buildTypesForTable(CqlTable table) {
    Set<GraphQLType> additionalTypes = new HashSet<>();

    additionalTypes.add(buildType(table));
    additionalTypes.add(buildInputType(table));
    additionalTypes.add(buildOrderType(table));
    additionalTypes.add(buildMutationResult(table));
    additionalTypes.add(buildFilterInput(table));
    additionalTypes.add(buildGroupByInput(table));
    return additionalTypes;
  }

  public GraphQLObjectType buildType(CqlTable table) {
    GraphQLObjectType.Builder builder =
        GraphQLObjectType.newObject()
            .name(nameMapping.getGraphqlName(table))
            .description(getTypeDescription(table, DmlType.QueryOutput));
    addToType(table.getPartitionKeyColumnsList(), table, builder);
    addToType(table.getClusteringKeyColumnsList(), table, builder);
    addToType(table.getColumnsList(), table, builder);
    addToType(table.getStaticColumnsList(), table, builder);

    buildAggregationFunctions(builder);

    return builder.build();
  }

  private void addToType(
      List<ColumnSpec> columns, CqlTable table, GraphQLObjectType.Builder builder) {
    for (ColumnSpec column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          GraphQLFieldDefinition.Builder fieldBuilder =
              new GraphQLFieldDefinition.Builder()
                  .name(graphqlName)
                  .type(fieldOutputTypes.get(column.getType()));
          builder.field(fieldBuilder.build());
        } catch (Exception e) {
          warn(
              e,
              "Could not create output type for column %s in table %s, skipping",
              column.getName(),
              table.getName());
        }
      }
    }
  }

  private String getTypeDescription(CqlTable table, DmlType dmlType) {
    StringBuilder builder = new StringBuilder();
    switch (dmlType) {
      case Input:
        builder.append("The input type");
        break;
      case FilterInput:
        builder.append("The input type used for filtering with non-equality operators");
        break;
      case Order:
        builder.append("The enum used to order a query result based on one or more fields");
        break;
      case QueryOutput:
        builder.append("The type used to represent results of a query");
        break;
      case MutationOutput:
        builder.append("The type used to represent results of a mutation");
        break;
      default:
        builder.append("Type");
        break;
    }

    builder.append(" for the table '");
    builder.append(table.getName());
    builder.append("'.");

    if (dmlType == DmlType.Input || dmlType == DmlType.FilterInput) {
      primaryKeyDescription(table, builder);
    }

    return builder.toString();
  }

  private void primaryKeyDescription(CqlTable table, StringBuilder builder) {
    // Include partition key information that is relevant to making a query
    List<String> primaryKeys =
        Stream.concat(
                table.getPartitionKeyColumnsList().stream(),
                table.getClusteringKeyColumnsList().stream())
            .map(c -> nameMapping.getGraphqlName(table, c))
            .collect(Collectors.toList());
    builder.append("\nNote that ").append("'").append(primaryKeys.get(0)).append("'");
    for (int i = 1; i < primaryKeys.size(); i++) {
      if (i == primaryKeys.size() - 1) {
        builder.append(" and ");
      } else {
        builder.append(", ");
      }
      builder.append("'").append(primaryKeys.get(i)).append("'");
    }
    if (primaryKeys.size() > 1) {
      builder.append(" are the fields that correspond to the table primary key.");
    } else {
      builder.append(" is the field that corresponds to the table primary key.");
    }
  }

  private String primaryKeyDescription(CqlTable table) {
    StringBuilder builder = new StringBuilder();
    primaryKeyDescription(table, builder);
    return builder.toString();
  }

  private void buildAggregationFunctions(GraphQLObjectType.Builder builder) {
    builder.field(buildFunctionField(SupportedGraphqlFunction.INT_FUNCTION, GraphQLInt));
    // The GraphQLFloat corresponds to CQL double
    builder.field(buildFunctionField(SupportedGraphqlFunction.DOUBLE_FUNCTION, GraphQLFloat));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.BIGINT_FUNCTION, CqlScalar.BIGINT.getGraphqlType()));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.DECIMAL_FUNCTION, CqlScalar.DECIMAL.getGraphqlType()));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.VARINT_FUNCTION, CqlScalar.VARINT.getGraphqlType()));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.FLOAT_FUNCTION, CqlScalar.FLOAT.getGraphqlType()));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.SMALLINT_FUNCTION, CqlScalar.SMALLINT.getGraphqlType()));
    builder.field(
        buildFunctionField(
            SupportedGraphqlFunction.TINYINT_FUNCTION, CqlScalar.TINYINT.getGraphqlType()));
  }

  private GraphQLFieldDefinition buildFunctionField(
      SupportedGraphqlFunction graphqlFunction, GraphQLScalarType returnType) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(graphqlFunction.getName())
        .description(
            String.format("Invocation of an aggregate function that returns %s.", returnType))
        .argument(
            GraphQLArgument.newArgument()
                .name("name")
                .description("Name of the function to invoke")
                .type(new GraphQLNonNull(GraphQLString)))
        .argument(
            GraphQLArgument.newArgument()
                .name("args")
                .description("Arguments passed to a function. It can be a list of column names.")
                .type(new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(GraphQLString)))))
        .type(returnType)
        .build();
  }

  private GraphQLType buildInputType(CqlTable table) {
    GraphQLInputObjectType.Builder input =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getGraphqlName(table) + "Input")
            .description(getTypeDescription(table, DmlType.Input));

    addToInputType(table.getPartitionKeyColumnsList(), table, input);
    addToInputType(table.getClusteringKeyColumnsList(), table, input);
    addToInputType(table.getColumnsList(), table, input);
    addToInputType(table.getStaticColumnsList(), table, input);
    return input.build();
  }

  private void addToInputType(
      List<ColumnSpec> columns, CqlTable table, GraphQLInputObjectType.Builder input) {
    for (ColumnSpec column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          GraphQLInputObjectField field =
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlName)
                  .type(fieldInputTypes.get(column.getType()))
                  .build();
          input.field(field);
        } catch (Exception e) {
          warn(
              e,
              "Could not create input type for column %s in table %s, skipping",
              column.getName(),
              table.getName());
        }
      }
    }
  }

  private GraphQLType buildOrderType(CqlTable table) {
    GraphQLEnumType.Builder input =
        GraphQLEnumType.newEnum()
            .name(nameMapping.getGraphqlName(table) + "Order")
            .description(getTypeDescription(table, DmlType.Order));

    addToOrderType(table.getPartitionKeyColumnsList(), table, input);
    addToOrderType(table.getClusteringKeyColumnsList(), table, input);
    addToOrderType(table.getColumnsList(), table, input);
    addToOrderType(table.getStaticColumnsList(), table, input);
    return input.build();
  }

  private void addToOrderType(
      List<ColumnSpec> columns, CqlTable table, GraphQLEnumType.Builder input) {
    for (ColumnSpec column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        input.value(graphqlName + "_DESC");
        input.value(graphqlName + "_ASC");
      }
    }
  }

  private GraphQLOutputType buildMutationResult(CqlTable table) {
    return GraphQLObjectType.newObject()
        .name(nameMapping.getGraphqlName(table) + "MutationResult")
        .description(getTypeDescription(table, DmlType.MutationOutput))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("applied")
                .type(Scalars.GraphQLBoolean))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("accepted")
                .description(
                    String.format(
                        "This field is relevant and fulfilled with data, only when used with the @%s directive",
                        SchemaConstants.ASYNC_DIRECTIVE))
                .type(Scalars.GraphQLBoolean))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("value")
                .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table))))
        .build();
  }

  private GraphQLType buildFilterInput(CqlTable table) {
    return GraphQLInputObjectType.newInputObject()
        .name(nameMapping.getGraphqlName(table) + "FilterInput")
        .description(getTypeDescription(table, DmlType.FilterInput))
        .fields(buildFilterInputFields(table))
        .build();
  }

  private List<GraphQLInputObjectField> buildFilterInputFields(CqlTable table) {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    addToFilterInputFields(table.getPartitionKeyColumnsList(), table, fields);
    addToFilterInputFields(table.getClusteringKeyColumnsList(), table, fields);
    addToFilterInputFields(table.getColumnsList(), table, fields);
    addToFilterInputFields(table.getStaticColumnsList(), table, fields);
    return fields;
  }

  private void addToFilterInputFields(
      List<ColumnSpec> columns, CqlTable table, List<GraphQLInputObjectField> fields) {
    for (ColumnSpec column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          fields.add(
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlName)
                  .type(fieldFilterInputTypes.get(column.getType()))
                  .build());
        } catch (Exception e) {
          warn(
              e,
              "Could not create filter input type for column %s in table %s, skipping",
              column.getName(),
              table.getName());
        }
      }
    }
  }

  private GraphQLType buildGroupByInput(CqlTable table) {
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getGraphqlName(table) + "GroupByInput");
    addToGroupByInput(table.getPartitionKeyColumnsList(), table, builder);
    addToGroupByInput(table.getClusteringKeyColumnsList(), table, builder);
    return builder.build();
  }

  private void addToGroupByInput(
      List<ColumnSpec> columns, CqlTable table, GraphQLInputObjectType.Builder builder) {
    for (ColumnSpec column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        builder.field(
            GraphQLInputObjectField.newInputObjectField()
                .name(graphqlName)
                .type(Scalars.GraphQLBoolean)
                .build());
      }
    }
  }

  private List<GraphQLFieldDefinition> buildQuery(CqlTable table) {
    String graphqlName = nameMapping.getGraphqlName(table);
    GraphQLFieldDefinition query =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(graphqlName)
            .description(
                String.format(
                    "Query for the table '%s'.%s", table.getName(), primaryKeyDescription(table)))
            .argument(
                GraphQLArgument.newArgument()
                    .name("value")
                    .type(new GraphQLTypeReference(graphqlName + "Input")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(new GraphQLTypeReference(graphqlName + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(new GraphQLList(new GraphQLTypeReference(graphqlName + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("groupBy")
                    .description("The columns to group results by.")
                    .type(new GraphQLTypeReference(graphqlName + "GroupByInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(keyspaceName, table, nameMapping))
            .build();

    GraphQLFieldDefinition filterQuery =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(graphqlName + "Filter")
            .deprecate("No longer supported. Use root type instead.")
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(new GraphQLTypeReference(graphqlName + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(new GraphQLList(new GraphQLTypeReference(graphqlName + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(keyspaceName, table, nameMapping))
            .build();

    return ImmutableList.of(query, filterQuery);
  }

  private GraphQLOutputType buildEntityResultOutput(CqlTable table) {
    if (entityResultMap.containsKey(table)) {
      return entityResultMap.get(table);
    }

    GraphQLOutputType entityResultType =
        GraphQLObjectType.newObject()
            .name(nameMapping.getGraphqlName(table) + "Result")
            .field(
                GraphQLFieldDefinition.newFieldDefinition().name("pageState").type(GraphQLString))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("values")
                    .type(
                        new GraphQLList(
                            new GraphQLNonNull(
                                new GraphQLTypeReference(nameMapping.getGraphqlName(table))))))
            .build();

    entityResultMap.put(table, entityResultType);

    return entityResultType;
  }

  private GraphQLFieldDefinition buildWarnings() {
    StringBuilder description =
        new StringBuilder("Warnings encountered during the CQL to GraphQL conversion.");
    if (warnings.isEmpty()) {
      description.append("\nNo warnings found, this will return an empty list.");
    } else {
      description.append("\nThis will return:");
      for (String warning : warnings) {
        description.append("\n- ").append(warning);
      }
    }
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("conversionWarnings")
        .description(description.toString())
        .type(list(GraphQLString))
        .dataFetcher((d) -> warnings)
        .build();
  }

  private GraphQLType buildQueryOptionsInputType() {
    return GraphQLInputObjectType.newInputObject()
        .name("QueryOptions")
        .description("The execution options for the query.")
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
                .defaultValue(CassandraFetcher.DEFAULT_CONSISTENCY.getValue().name())
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
                .defaultValue(CassandraFetcher.DEFAULT_PAGE_SIZE)
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("pageState")
                .type(Scalars.GraphQLString)
                .build())
        .build();
  }

  private GraphQLObjectType buildQueries(List<GraphQLFieldDefinition> queryFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");

    for (GraphQLFieldDefinition fieldDefinition : queryFields) {
      builder.field(fieldDefinition);
    }

    return builder.build();
  }

  private List<GraphQLFieldDefinition> buildMutations(CqlTable table) {
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();
    mutationFields.add(buildDelete(table));
    mutationFields.add(buildInsert(table));
    mutationFields.add(buildBulkInsert(table));
    mutationFields.add(buildUpdate(table));
    return mutationFields;
  }

  private GraphQLFieldDefinition buildDelete(CqlTable table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("delete" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Delete mutation for the table '%s'.%s",
                table.getName(), primaryKeyDescription(table)))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult"))
        .dataFetcher(new DeleteMutationFetcher(keyspaceName, table, nameMapping))
        .build();
  }

  private GraphQLFieldDefinition buildInsert(CqlTable table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("insert" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Insert mutation for the table '%s'.%s",
                table.getName(), primaryKeyDescription(table)))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult"))
        .dataFetcher(new InsertMutationFetcher(keyspaceName, table, nameMapping))
        .build();
  }

  private GraphQLFieldDefinition buildBulkInsert(CqlTable table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("bulkInsert" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Bulk insert mutations for the table '%s'.%s",
                table.getName(), primaryKeyDescription(table)))
        .argument(
            GraphQLArgument.newArgument()
                .name("values")
                .type(
                    new GraphQLList(
                        new GraphQLNonNull(
                            new GraphQLTypeReference(
                                nameMapping.getGraphqlName(table) + "Input")))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(
            new GraphQLList(
                new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult")))
        .dataFetcher(new BulkInsertMutationFetcher(keyspaceName, table, nameMapping))
        .build();
  }

  private GraphQLFieldDefinition buildUpdate(CqlTable table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("update" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Update mutation for the table '%s'.%s",
                table.getName(), primaryKeyDescription(table)))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult"))
        .dataFetcher(new UpdateMutationFetcher(keyspaceName, table, nameMapping))
        .build();
  }

  private void addAtomicDirective(GraphQLSchema.Builder builder) {
    builder.additionalDirective(
        GraphQLDirective.newDirective()
            .validLocation(Introspection.DirectiveLocation.MUTATION)
            .name(SchemaConstants.ATOMIC_DIRECTIVE)
            .description("Instructs the server to apply the mutations in a LOGGED batch")
            .build());
  }

  private void addAsyncDirective(GraphQLSchema.Builder builder) {
    builder.additionalDirective(
        GraphQLDirective.newDirective()
            .validLocation(Introspection.DirectiveLocation.MUTATION)
            .name(SchemaConstants.ASYNC_DIRECTIVE)
            .description(
                "Instructs the server to apply the mutations asynchronously without waiting for the result.")
            .build());
  }

  private GraphQLObjectType buildMutationRoot(List<GraphQLFieldDefinition> mutationFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
    for (GraphQLFieldDefinition mutation : mutationFields) {
      builder.field(mutation);
    }
    return builder.build();
  }

  @FormatMethod
  private void warn(Exception e, @FormatString String format, Object... arguments) {
    String message = String.format(format, arguments);
    warnings.add(message + " (" + e.getMessage() + ")");
    if (!(e instanceof SchemaWarningException)) {
      LOG.warn(message, e);
    }
  }
}
