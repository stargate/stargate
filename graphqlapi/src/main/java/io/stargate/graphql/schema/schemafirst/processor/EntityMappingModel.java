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
package io.stargate.graphql.schema.schemafirst.processor;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityMappingModel {

  private final String graphqlName;
  private final String keyspaceName;
  private final String cqlName;
  private final Target target;
  private final List<FieldMappingModel> partitionKey;
  private final List<FieldMappingModel> clusteringColumns;
  private final List<FieldMappingModel> primaryKey;
  private final List<FieldMappingModel> regularColumns;
  private final List<FieldMappingModel> allColumns;
  private final boolean isFederated;
  private final Optional<String> inputTypeName;

  EntityMappingModel(
      String graphqlName,
      String keyspaceName,
      String cqlName,
      Target target,
      List<FieldMappingModel> partitionKey,
      List<FieldMappingModel> clusteringColumns,
      List<FieldMappingModel> regularColumns,
      boolean isFederated,
      Optional<String> inputTypeName) {
    this.graphqlName = graphqlName;
    this.keyspaceName = keyspaceName;
    this.cqlName = cqlName;
    this.target = target;
    this.partitionKey = ImmutableList.copyOf(partitionKey);
    this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
    this.isFederated = isFederated;
    this.inputTypeName = inputTypeName;
    this.primaryKey =
        ImmutableList.<FieldMappingModel>builder()
            .addAll(partitionKey)
            .addAll(clusteringColumns)
            .build();
    this.regularColumns = ImmutableList.copyOf(regularColumns);
    this.allColumns =
        ImmutableList.<FieldMappingModel>builder()
            .addAll(primaryKey)
            .addAll(regularColumns)
            .build();
  }

  public String getGraphqlName() {
    return graphqlName;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getCqlName() {
    return cqlName;
  }

  public Target getTarget() {
    return target;
  }

  public List<FieldMappingModel> getPartitionKey() {
    return partitionKey;
  }

  public List<FieldMappingModel> getClusteringColumns() {
    return clusteringColumns;
  }

  /**
   * The full primary key (partition key + clustering columns), that uniquely identifies a CQL row.
   */
  public List<FieldMappingModel> getPrimaryKey() {
    return primaryKey;
  }

  public List<FieldMappingModel> getRegularColumns() {
    return regularColumns;
  }

  public List<FieldMappingModel> getAllColumns() {
    return allColumns;
  }

  public boolean isFederated() {
    return isFederated;
  }

  public Optional<String> getInputTypeName() {
    return inputTypeName;
  }

  AbstractBound<?> buildCreateQuery(QueryBuilder builder) {
    return target.buildCreateQuery(builder, this);
  }

  AbstractBound<?> buildDropQuery(QueryBuilder builder) {
    return target.buildDropQuery(builder, this);
  }

  static Optional<EntityMappingModel> build(ObjectTypeDefinition type, ProcessingContext context) {

    String graphqlName = type.getName();
    Optional<Directive> cqlDirective = DirectiveHelper.getDirective("cql_entity", type);
    String cqlName =
        cqlDirective
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "name", context))
            .orElse(graphqlName);

    Target target =
        cqlDirective
            .flatMap(d -> DirectiveHelper.getEnumArgument(d, "target", Target.class, context))
            .orElse(Target.TABLE);

    List<FieldMappingModel> partitionKey = new ArrayList<>();
    List<FieldMappingModel> clusteringColumns = new ArrayList<>();
    List<FieldMappingModel> regularColumns = new ArrayList<>();

    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      FieldMappingModel.build(fieldDefinition, context, graphqlName, target)
          .ifPresent(
              fieldMapping -> {
                if (fieldMapping.isPartitionKey()) {
                  partitionKey.add(fieldMapping);
                } else if (fieldMapping.getClusteringOrder().isPresent()) {
                  clusteringColumns.add(fieldMapping);
                } else {
                  regularColumns.add(fieldMapping);
                }
              });
    }

    // Check that we have the necessary kinds of columns depending on the target:
    switch (target) {
      case TABLE:
        if (partitionKey.isEmpty()) {
          FieldMappingModel firstField = regularColumns.get(0);
          if (TypeHelper.isGraphqlId(firstField.getGraphqlType())) {
            context.addInfo(
                type.getSourceLocation(),
                "%s: using %s as the partition key, "
                    + "because it has type ID and no other fields are annotated",
                graphqlName,
                firstField.getGraphqlName());
            partitionKey.add(firstField.asPartitionKey());
            regularColumns.remove(firstField);
          } else {
            context.addError(
                type.getSourceLocation(),
                ProcessingMessageType.InvalidMapping,
                "%s must have at least one partition key field "
                    + "(use scalar type ID, or annotate your fields with @cql_column(partitionKey: true))",
                graphqlName);
            return Optional.empty();
          }
        }
        break;
      case UDT:
        if (regularColumns.isEmpty()) {
          context.addError(
              type.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "%s must have at least one field",
              graphqlName);
          return Optional.empty();
        }
        break;
    }

    Optional<String> inputTypeName =
        DirectiveHelper.getDirective("cql_input", type)
            .map(
                d -> {
                  Optional<String> maybeName =
                      DirectiveHelper.getStringArgument(d, "name", context);
                  if (maybeName.isPresent()) {
                    return maybeName.get();
                  } else {
                    context.addInfo(
                        d.getSourceLocation(),
                        "%1$s: using '%1$sInput' as the input type name since @cql_input doesn't "
                            + "have an argument",
                        graphqlName);
                    return graphqlName + "Input";
                  }
                });

    // Check that if the @key directive is present, it matches the CQL primary key:
    List<Directive> keyDirectives = type.getDirectives("key");
    boolean isFederated;
    if (!keyDirectives.isEmpty()) {
      if (target == Target.UDT) {
        context.addError(
            type.getSourceLocation(),
            ProcessingMessageType.InvalidMapping,
            "%s: can't use @key directive because this type maps to a UDT",
            graphqlName);
        return Optional.empty();
      }
      if (keyDirectives.size() > 1) {
        context.addError(
            type.getSourceLocation(),
            ProcessingMessageType.InvalidMapping,
            "%s: this implementation only supports a single @key directive",
            graphqlName);
        return Optional.empty();
      }
      Directive keyDirective = keyDirectives.get(0);
      Optional<String> fieldsArgument =
          DirectiveHelper.getStringArgument(keyDirective, "fields", context);
      if (fieldsArgument.isPresent()) {
        String value = fieldsArgument.get();
        if (!NON_NESTED_FIELDS.matcher(value).matches()) {
          context.addError(
              keyDirective.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "%s: could not parse @key.fields "
                  + "(this implementation only supports top-level fields as key components)",
              graphqlName);
          return Optional.empty();
        }
        Set<String> directiveFields = ImmutableSet.copyOf(ON_SPACES.split(value));
        Set<String> primaryKeyFields =
            Stream.concat(partitionKey.stream(), clusteringColumns.stream())
                .map(FieldMappingModel::getGraphqlName)
                .collect(Collectors.toSet());
        if (!directiveFields.equals(primaryKeyFields)) {
          context.addError(
              keyDirective.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "%s: @key.fields doesn't match the partition and clustering keys (expected %s)",
              graphqlName,
              primaryKeyFields);
          return Optional.empty();
        }
        isFederated = true;
      } else {
        context.addError(
            keyDirective.getSourceLocation(),
            ProcessingMessageType.InvalidSyntax,
            "%s: @key directive must have a 'fields' argument",
            graphqlName);
        return Optional.empty();
      }
    } else {
      isFederated = false;
    }

    return Optional.of(
        new EntityMappingModel(
            graphqlName,
            context.getKeyspace().name(),
            cqlName,
            target,
            partitionKey,
            clusteringColumns,
            regularColumns,
            isFederated,
            inputTypeName));
  }

  enum Target {
    TABLE {
      @Override
      AbstractBound<?> buildCreateQuery(QueryBuilder builder, EntityMappingModel entity) {

        List<Column> partitionKeyColumns =
            entity.getPartitionKey().stream()
                .map(
                    field ->
                        Column.create(
                            field.getCqlName(), Column.Kind.PartitionKey, field.getCqlType()))
                .collect(Collectors.toList());
        List<Column> clusteringColumns =
            entity.getClusteringColumns().stream()
                .map(
                    field -> {
                      // We only put fields here if they have an order:
                      assert field.getClusteringOrder().isPresent();
                      return Column.create(
                          field.getCqlName(),
                          Column.Kind.Clustering,
                          field.getCqlType(),
                          field.getClusteringOrder().get());
                    })
                .collect(Collectors.toList());
        List<Column> regularColumns =
            entity.getRegularColumns().stream()
                .map(field -> Column.create(field.getCqlName(), field.getCqlType()))
                .collect(Collectors.toList());

        return builder
            .create()
            .table(entity.getKeyspaceName(), entity.getCqlName())
            .column(partitionKeyColumns)
            .column(clusteringColumns)
            .column(regularColumns)
            .build()
            .bind();
      }

      @Override
      public AbstractBound<?> buildDropQuery(QueryBuilder builder, EntityMappingModel entity) {
        return builder
            .drop()
            .table(entity.getKeyspaceName(), entity.getCqlName())
            .ifExists()
            .build()
            .bind();
      }
    },
    UDT {
      @Override
      AbstractBound<?> buildCreateQuery(QueryBuilder builder, EntityMappingModel entity) {

        // Processing should have errored out before we get here if this is the case
        assert entity.getPartitionKey().isEmpty() && entity.getClusteringColumns().isEmpty();

        List<Column> columns =
            entity.getRegularColumns().stream()
                .map(field -> Column.create(field.getCqlName(), field.getCqlType()))
                .collect(Collectors.toList());

        UserDefinedType udt =
            ImmutableUserDefinedType.builder()
                .keyspace(entity.getKeyspaceName())
                .name(entity.getCqlName())
                .columns(columns)
                .build();

        return builder.create().type(entity.getKeyspaceName(), udt).build().bind();
      }

      @Override
      public AbstractBound<?> buildDropQuery(QueryBuilder builder, EntityMappingModel entity) {
        return builder
            .drop()
            .type(
                entity.getKeyspaceName(),
                ImmutableUserDefinedType.builder()
                    .keyspace(entity.getKeyspaceName())
                    .name(entity.getCqlName())
                    .build())
            .ifExists()
            .build()
            .bind();
      }
    },
    ;

    abstract AbstractBound<?> buildCreateQuery(QueryBuilder builder, EntityMappingModel entity);

    abstract AbstractBound<?> buildDropQuery(QueryBuilder builder, EntityMappingModel entity);
  }

  private static final Pattern NON_NESTED_FIELDS =
      Pattern.compile("[_A-Za-z][_0-9A-Za-z]*(?:\\s+[_A-Za-z][_0-9A-Za-z]*)*");
  private static final Splitter ON_SPACES = Splitter.onPattern("\\s+");
}
