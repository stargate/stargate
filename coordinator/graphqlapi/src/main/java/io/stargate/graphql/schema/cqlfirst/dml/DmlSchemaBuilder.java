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
package io.stargate.graphql.schema.cqlfirst.dml;

import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLList.list;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.BIGINT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.DECIMAL_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.DOUBLE_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.FLOAT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.INT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.SMALLINT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.TINYINT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction.VARINT_FUNCTION;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.Scalars;
import graphql.introspection.Introspection;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumType.Builder;
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
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.SchemaConstants;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.BulkInsertMutationFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.DeleteMutationFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.InsertMutationFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.QueryFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.UpdateMutationFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedGraphqlFunction;
import io.stargate.graphql.schema.scalars.CqlScalar;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DmlSchemaBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DmlSchemaBuilder.class);
  private static final String STARGATE_MUTATION_CONSISTENCY_LEVELS =
      "stargate.graphql.mutation_consistency_levels";
  private static final String STARGATE_QUERY_CONSISTENCY_LEVELS =
      "stargate.graphql.query_consistency_levels";

  private final Map<Table, GraphQLOutputType> entityResultMap = new HashMap<>();
  private final List<String> warnings = new ArrayList<>();
  private final FieldInputTypeCache fieldInputTypes;
  private final FieldOutputTypeCache fieldOutputTypes;
  private final FieldFilterInputTypeCache fieldFilterInputTypes;
  private final NameMapping nameMapping;
  private final Keyspace keyspace;
  private static final GraphQLInputType MUTATION_OPTIONS = initializeMutationOptions();

  /** Describes the different kind of types generated from a table */
  private enum DmlType {
    QueryOutput,
    MutationOutput,
    Input,
    FilterInput,
    Order
  }

  public DmlSchemaBuilder(Keyspace keyspace) {

    this.keyspace = keyspace;

    this.nameMapping = new NameMapping(keyspace.tables(), keyspace.userDefinedTypes(), warnings);
    this.fieldInputTypes = new FieldInputTypeCache(this.nameMapping, warnings);
    this.fieldOutputTypes = new FieldOutputTypeCache(this.nameMapping, warnings);
    this.fieldFilterInputTypes =
        new FieldFilterInputTypeCache(this.fieldInputTypes, this.nameMapping);
  }

  @SuppressWarnings("deprecation")
  public GraphQLSchema build() {
    GraphQLSchema.Builder builder = new GraphQLSchema.Builder();

    List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();

    // Tables must be iterated one at a time. If a table is unfulfillable, it is skipped
    for (Table table : keyspace.tables()) {
      if (nameMapping.getGraphqlName(table) == null) {
        // This means there was a name clash. We already added a warning in NameMapping.
        continue;
      }
      Set<GraphQLType> additionalTypes;
      List<GraphQLFieldDefinition> tableQueryField;
      List<GraphQLFieldDefinition> tableMutationFields;

      try {
        additionalTypes = buildTypesForTable(table);
        tableQueryField = buildQuery(table);
        tableMutationFields = buildMutations(table);
      } catch (Exception e) {
        warn(e, "Could not convert table %s, skipping", table.name());
        continue;
      }

      builder.additionalTypes(additionalTypes);
      queryFields.addAll(tableQueryField);
      mutationFields.addAll(tableMutationFields);
    }

    addAtomicDirective(builder);

    addAsyncDirective(builder);

    if (queryFields.isEmpty()) {
      GraphQLFieldDefinition emptyQueryField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("keyspaceEmptyQuery")
              .description("Placeholder query that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      queryFields.add(emptyQueryField);
    }

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

    queryFields.add(buildWarnings());

    builder.additionalType(buildQueryOptionsInputType());
    builder.query(buildQueries(queryFields));
    builder.mutation(buildMutationRoot(mutationFields));
    return builder.build();
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

  private GraphQLObjectType buildQueries(List<GraphQLFieldDefinition> queryFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");

    for (GraphQLFieldDefinition fieldDefinition : queryFields) {
      builder.field(fieldDefinition);
    }

    return builder.build();
  }

  private List<GraphQLFieldDefinition> buildQuery(Table table) {
    String graphqlName = nameMapping.getGraphqlName(table);
    GraphQLFieldDefinition query =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(graphqlName)
            .description(
                String.format(
                    "Query for the table '%s'.%s", table.name(), primaryKeyDescription(table)))
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
            .dataFetcher(new QueryFetcher(table, nameMapping))
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
            .dataFetcher(new QueryFetcher(table, nameMapping))
            .build();

    return ImmutableList.of(query, filterQuery);
  }

  private List<GraphQLFieldDefinition> buildMutations(Table table) {
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();
    mutationFields.add(buildDelete(table));
    mutationFields.add(buildInsert(table));
    mutationFields.add(buildBulkInsert(table));
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
    additionalTypes.add(buildGroupByInput(table));
    return additionalTypes;
  }

  private GraphQLType buildFilterInput(Table table) {
    return GraphQLInputObjectType.newInputObject()
        .name(nameMapping.getGraphqlName(table) + "FilterInput")
        .description(getTypeDescription(table, DmlType.FilterInput))
        .fields(buildFilterInputFields(table))
        .build();
  }

  private GraphQLType buildGroupByInput(Table table) {
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getGraphqlName(table) + "GroupByInput");
    for (Column column : table.primaryKeyColumns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        builder.field(
            GraphQLInputObjectField.newInputObjectField()
                .name(graphqlName)
                .type(Scalars.GraphQLBoolean)
                .build());
      }
    }
    return builder.build();
  }

  private GraphQLFieldDefinition buildUpdate(Table table) {

    return GraphQLFieldDefinition.newFieldDefinition()
        .name("update" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Update mutation for the table '%s'.%s",
                table.name(), primaryKeyDescription(table)))
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
        .dataFetcher(new UpdateMutationFetcher(table, nameMapping))
        .build();
  }

  private static GraphQLInputType initializeMutationOptions() {
    String consistencyLevelsStr =
        System.getProperty(STARGATE_MUTATION_CONSISTENCY_LEVELS, "LOCAL_ONE,LOCAL_QUORUM,ALL");

    GraphQLEnumType consistencyEnumBuilder =
        getConsistencyEnum(consistencyLevelsStr, "MutationConsistency");

    return GraphQLInputObjectType.newInputObject()
        .name("MutationOptions")
        .description("The execution options for the mutation.")
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("consistency")
                .type(consistencyEnumBuilder)
                .defaultValue(CassandraFetcher.DEFAULT_CONSISTENCY.toString())
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
                .defaultValue(CassandraFetcher.DEFAULT_SERIAL_CONSISTENCY.toString())
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("ttl")
                .type(Scalars.GraphQLInt)
                .build())
        .build();
  }

  private GraphQLFieldDefinition buildBulkInsert(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("bulkInsert" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Bulk insert mutations for the table '%s'.%s",
                table.name(), primaryKeyDescription(table)))
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
        .dataFetcher(new BulkInsertMutationFetcher(table, nameMapping))
        .build();
  }

  private GraphQLFieldDefinition buildInsert(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("insert" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Insert mutation for the table '%s'.%s",
                table.name(), primaryKeyDescription(table)))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult"))
        .dataFetcher(new InsertMutationFetcher(table, nameMapping))
        .build();
  }

  private GraphQLFieldDefinition buildDelete(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("delete" + nameMapping.getGraphqlName(table))
        .description(
            String.format(
                "Delete mutation for the table '%s'.%s",
                table.name(), primaryKeyDescription(table)))
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
        .dataFetcher(new DeleteMutationFetcher(table, nameMapping))
        .build();
  }

  private List<GraphQLInputObjectField> buildFilterInputFields(Table table) {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    for (Column column : table.columns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          fields.add(
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlName)
                  .type(fieldFilterInputTypes.get(column.type()))
                  .build());
        } catch (Exception e) {
          warn(
              e,
              "Could not create filter input type for column %s in table %s, skipping",
              column.name(),
              column.table());
        }
      }
    }
    return fields;
  }

  private GraphQLOutputType buildMutationResult(Table table) {
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

  private GraphQLInputObjectType buildQueryOptionsInputType() {
    String consistencyLevelsStr =
        System.getProperty(
            STARGATE_QUERY_CONSISTENCY_LEVELS, "LOCAL_ONE,LOCAL_QUORUM,ALL,SERIAL,LOCAL_SERIAL");

    GraphQLEnumType consistencyEnumBuilder =
        getConsistencyEnum(consistencyLevelsStr, "QueryConsistency");

    return GraphQLInputObjectType.newInputObject()
        .name("QueryOptions")
        .description("The execution options for the query.")
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("consistency")
                .type(consistencyEnumBuilder)
                .defaultValue(CassandraFetcher.DEFAULT_CONSISTENCY.toString())
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

  @VisibleForTesting
  protected static GraphQLEnumType getConsistencyEnum(
      String consistencyLevelsStr, String enumName) {
    String[] consistencyLevels = consistencyLevelsStr.split(",");

    Builder consistencyEnumBuilder = GraphQLEnumType.newEnum().name(enumName);
    boolean defaultAdded = false;

    for (String level : consistencyLevels) {
      try {
        level = level.toUpperCase(Locale.ENGLISH);
        ConsistencyLevel.valueOf(level);
        consistencyEnumBuilder.value(level);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid consistency level {} specified, skipping", level);
      }
      if (level.equals(CassandraFetcher.DEFAULT_CONSISTENCY.toString())) {
        defaultAdded = true;
      }
    }
    // Always add the default consistency level regardless of system params
    // else the default won't work
    if (!defaultAdded) {
      consistencyEnumBuilder.value(CassandraFetcher.DEFAULT_CONSISTENCY.toString());
    }
    return consistencyEnumBuilder.build();
  }

  private GraphQLType buildOrderType(Table table) {
    GraphQLEnumType.Builder input =
        GraphQLEnumType.newEnum()
            .name(nameMapping.getGraphqlName(table) + "Order")
            .description(getTypeDescription(table, DmlType.Order));

    for (Column column : table.columns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        input.value(graphqlName + "_DESC");
        input.value(graphqlName + "_ASC");
      }
    }
    return input.build();
  }

  private GraphQLType buildInputType(Table table) {
    GraphQLInputObjectType.Builder input =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getGraphqlName(table) + "Input")
            .description(getTypeDescription(table, DmlType.Input));

    for (Column column : table.columns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          GraphQLInputObjectField field =
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlName)
                  .type(fieldInputTypes.get(column.type()))
                  .build();
          input.field(field);
        } catch (Exception e) {
          warn(
              e,
              "Could not create input type for column %s in table %s, skipping",
              column.name(),
              column.table());
        }
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

  public GraphQLObjectType buildType(Table table) {
    GraphQLObjectType.Builder builder =
        GraphQLObjectType.newObject()
            .name(nameMapping.getGraphqlName(table))
            .description(getTypeDescription(table, DmlType.QueryOutput));
    for (Column column : table.columns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null) {
        try {
          GraphQLFieldDefinition.Builder fieldBuilder =
              new GraphQLFieldDefinition.Builder()
                  .name(graphqlName)
                  .type(fieldOutputTypes.get(column.type()));
          builder.field(fieldBuilder.build());
        } catch (Exception e) {
          warn(
              e,
              "Could not create output type for column %s in table %s, skipping",
              column.name(),
              column.table());
        }
      }
    }

    buildAggregationFunctions(builder);

    return builder.build();
  }

  private void buildAggregationFunctions(GraphQLObjectType.Builder builder) {
    builder.field(buildFunctionField(INT_FUNCTION, GraphQLInt));
    // The GraphQLFloat corresponds to CQL double
    builder.field(buildFunctionField(DOUBLE_FUNCTION, GraphQLFloat));
    builder.field(buildFunctionField(BIGINT_FUNCTION, CqlScalar.BIGINT.getGraphqlType()));
    builder.field(buildFunctionField(DECIMAL_FUNCTION, CqlScalar.DECIMAL.getGraphqlType()));
    builder.field(buildFunctionField(VARINT_FUNCTION, CqlScalar.VARINT.getGraphqlType()));
    builder.field(buildFunctionField(FLOAT_FUNCTION, CqlScalar.FLOAT.getGraphqlType()));
    builder.field(buildFunctionField(SMALLINT_FUNCTION, CqlScalar.SMALLINT.getGraphqlType()));
    builder.field(buildFunctionField(TINYINT_FUNCTION, CqlScalar.TINYINT.getGraphqlType()));
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

  @FormatMethod
  private void warn(Exception e, @FormatString String format, Object... arguments) {
    String message = String.format(format, arguments);
    warnings.add(message + " (" + e.getMessage() + ")");
    if (!(e instanceof SchemaWarningException)) {
      LOG.warn(message, e);
    }
  }

  private String getTypeDescription(Table table, DmlType dmlType) {
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
    builder.append(table.name());
    builder.append("'.");

    if (dmlType == DmlType.Input || dmlType == DmlType.FilterInput) {
      primaryKeyDescription(table, builder);
    }

    return builder.toString();
  }

  private void primaryKeyDescription(Table table, StringBuilder builder) {
    // Include partition key information that is relevant to making a query
    List<String> primaryKeys =
        Stream.concat(table.partitionKeyColumns().stream(), table.clusteringKeyColumns().stream())
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

  private String primaryKeyDescription(Table table) {
    StringBuilder builder = new StringBuilder();
    primaryKeyDescription(table, builder);
    return builder.toString();
  }
}
