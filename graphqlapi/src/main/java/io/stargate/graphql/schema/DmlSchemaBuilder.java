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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import io.stargate.auth.AuthnzService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.fetchers.dml.DeleteMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.InsertMutationFetcher;
import io.stargate.graphql.schema.fetchers.dml.QueryFetcher;
import io.stargate.graphql.schema.fetchers.dml.UpdateMutationFetcher;
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
  private final AuthnzService authnzService;
  private final Map<Table, GraphQLOutputType> entityResultMap = new HashMap<>();
  private final FieldInputTypeCache fieldInputTypes;
  private final FieldOutputTypeCache fieldOutputTypes;
  private final FieldFilterInputTypeCache fieldFilterInputTypes;
  private final NameMapping nameMapping;
  private final Keyspace keyspace;

  DmlSchemaBuilder(Persistence persistence, AuthnzService authnzService, Keyspace keyspace) {
    this.persistence = persistence;
    this.authnzService = authnzService;
    this.keyspace = keyspace;

    this.nameMapping = new NameMapping(keyspace.tables(), keyspace.userDefinedTypes());
    this.fieldInputTypes = new FieldInputTypeCache(this.nameMapping);
    this.fieldOutputTypes = new FieldOutputTypeCache(this.nameMapping);
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
                    .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getGraphqlName(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(table, nameMapping, persistence, authnzService))
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
                            nameMapping.getGraphqlName(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(new QueryFetcher(table, nameMapping, persistence, authnzService))
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
        .fields(buildFilterInputFields(table))
        .build();
  }

  private GraphQLFieldDefinition buildUpdate(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("update" + nameMapping.getGraphqlName(table))
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
        .dataFetcher(new UpdateMutationFetcher(table, nameMapping, persistence, authnzService))
        .build();
  }

  private GraphQLFieldDefinition buildInsert(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("insert" + nameMapping.getGraphqlName(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(MUTATION_OPTIONS))
        .type(new GraphQLTypeReference(nameMapping.getGraphqlName(table) + "MutationResult"))
        .dataFetcher(new InsertMutationFetcher(table, nameMapping, persistence, authnzService))
        .build();
  }

  private GraphQLFieldDefinition buildDelete(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("delete" + nameMapping.getGraphqlName(table))
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
        .dataFetcher(new DeleteMutationFetcher(table, nameMapping, persistence, authnzService))
        .build();
  }

  private List<GraphQLInputObjectField> buildFilterInputFields(Table table) {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    for (Column column : table.columns()) {
      fields.add(
          GraphQLInputObjectField.newInputObjectField()
              .name(nameMapping.getGraphqlName(table, column))
              .type(fieldFilterInputTypes.get(column.type()))
              .build());
    }

    Preconditions.checkState(
        !fields.isEmpty(), "Could not generate an input type for table, skipping.");

    return fields;
  }

  private GraphQLOutputType buildMutationResult(Table table) {
    return GraphQLObjectType.newObject()
        .name(nameMapping.getGraphqlName(table) + "MutationResult")
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
        GraphQLEnumType.newEnum().name(nameMapping.getGraphqlName(table) + "Order");
    for (Column columnMetadata : table.columns()) {
      input.value(nameMapping.getGraphqlName(table, columnMetadata) + "_DESC");
      input.value(nameMapping.getGraphqlName(table, columnMetadata) + "_ASC");
    }
    return input.build();
  }

  private GraphQLType buildInputType(Table table) {
    GraphQLInputObjectType.Builder input =
        GraphQLInputObjectType.newInputObject().name(nameMapping.getGraphqlName(table) + "Input");
    for (Column columnMetadata : table.columns()) {
      try {
        GraphQLInputObjectField field =
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getGraphqlName(table, columnMetadata))
                .type(fieldInputTypes.get(columnMetadata.type()))
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
        GraphQLObjectType.newObject().name(nameMapping.getGraphqlName(table));
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
        .name(nameMapping.getGraphqlName(table, columnMetadata))
        .type(fieldOutputTypes.get(columnMetadata.type()));
  }

  private static final GraphQLInputType MUTATION_OPTIONS =
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
                  .build())
          .build();
}
