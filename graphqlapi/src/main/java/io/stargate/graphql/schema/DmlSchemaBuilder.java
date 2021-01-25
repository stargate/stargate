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
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import io.stargate.graphql.schema.fetchers.dml.DeleteMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.InsertMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.QueryFetcher;
import io.stargate.graphql.schema.fetchers.dml.UpdateMutationFetcher;
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

class DmlSchemaBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DmlSchemaBuilder.class);

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final Map<Table, GraphQLOutputType> entityResultMap = new HashMap<>();
  private final List<String> warnings = new ArrayList<>();
  private final FieldInputTypeCache fieldInputTypes;
  private final FieldOutputTypeCache fieldOutputTypes;
  private final FieldFilterInputTypeCache fieldFilterInputTypes;
  private final NameMapping nameMapping;
  private final DataStoreFactory dataStoreFactory;
  private final Keyspace keyspace;

  /** Describes the different kind of types generated from a table */
  private enum DmlType {
    QueryOutput,
    MutationOutput,
    Input,
    FilterInput,
    Order
  }

  DmlSchemaBuilder(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      Keyspace keyspace,
      DataStoreFactory dataStoreFactory) {

    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.keyspace = keyspace;

    this.nameMapping = new NameMapping(keyspace.tables(), keyspace.userDefinedTypes(), warnings);
    this.dataStoreFactory = dataStoreFactory;
    this.fieldInputTypes = new FieldInputTypeCache(this.nameMapping, warnings);
    this.fieldOutputTypes = new FieldOutputTypeCache(this.nameMapping, warnings);
    this.fieldFilterInputTypes =
        new FieldFilterInputTypeCache(this.fieldInputTypes, this.nameMapping);
  }

  @SuppressWarnings("deprecation")
  GraphQLSchema build() {
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

    builder.additionalDirective(
        GraphQLDirective.newDirective()
            .validLocation(Introspection.DirectiveLocation.MUTATION)
            .name(SchemaConstants.ATOMIC_DIRECTIVE)
            .description("Instructs the server to apply the mutations in a LOGGED batch")
            .build());

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

    queryFields.add(buildWarnings());

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
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(
                new QueryFetcher(
                    table,
                    nameMapping,
                    authenticationService,
                    authorizationService,
                    dataStoreFactory))
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
            .dataFetcher(
                new QueryFetcher(
                    table,
                    nameMapping,
                    authenticationService,
                    authorizationService,
                    dataStoreFactory))
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
        .name(nameMapping.getGraphqlName(table) + "FilterInput")
        .description(getTypeDescription(table, DmlType.FilterInput))
        .fields(buildFilterInputFields(table))
        .build();
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
        .dataFetcher(
            new UpdateMutationFetcher(
                table, nameMapping, authenticationService, authorizationService, dataStoreFactory))
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
        .dataFetcher(
            new InsertMutationFetcher(
                table, nameMapping, authenticationService, authorizationService, dataStoreFactory))
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
        .dataFetcher(
            new DeleteMutationFetcher(
                table, nameMapping, authenticationService, authorizationService, dataStoreFactory))
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
                .name("value")
                .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table))))
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

    return builder.build();
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
