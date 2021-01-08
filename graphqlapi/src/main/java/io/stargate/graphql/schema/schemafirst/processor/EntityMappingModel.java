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

import com.google.common.collect.ImmutableList;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class EntityMappingModel {

  private final String graphqlName;
  private final String keyspaceName;
  private final String cqlName;
  private final Target target;
  private final List<FieldMappingModel> partitionKey;
  private final List<FieldMappingModel> clusteringColumns;
  private final List<FieldMappingModel> regularColumns;

  EntityMappingModel(
      String graphqlName,
      String keyspaceName,
      String cqlName,
      Target target,
      List<FieldMappingModel> partitionKey,
      List<FieldMappingModel> clusteringColumns,
      List<FieldMappingModel> regularColumns) {
    this.graphqlName = graphqlName;
    this.keyspaceName = keyspaceName;
    this.cqlName = cqlName;
    this.target = target;
    this.partitionKey = ImmutableList.copyOf(partitionKey);
    this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
    this.regularColumns = ImmutableList.copyOf(regularColumns);
  }

  String getGraphqlName() {
    return graphqlName;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  String getCqlName() {
    return cqlName;
  }

  Target getTarget() {
    return target;
  }

  List<FieldMappingModel> getPartitionKey() {
    return partitionKey;
  }

  List<FieldMappingModel> getClusteringColumns() {
    return clusteringColumns;
  }

  List<FieldMappingModel> getPrimaryKey() {
    return ImmutableList.<FieldMappingModel>builder()
        .addAll(partitionKey)
        .addAll(clusteringColumns)
        .build();
  }

  List<FieldMappingModel> getRegularColumns() {
    return regularColumns;
  }

  List<FieldMappingModel> getAllColumns() {
    return ImmutableList.<FieldMappingModel>builder()
        .addAll(partitionKey)
        .addAll(clusteringColumns)
        .addAll(regularColumns)
        .build();
  }

  AbstractBound<?> buildCreateQuery(QueryBuilder builder) {
    return target.buildCreateQuery(builder, this);
  }

  AbstractBound<?> buildDropQuery(QueryBuilder builder) {
    return target.buildDropQuery(builder, this);
  }

  static Optional<EntityMappingModel> build(ObjectTypeDefinition type, ProcessingContext context) {

    String graphqlName = type.getName();
    Optional<Directive> directive = DirectiveHelper.getDirective("cqlEntity", type);
    String cqlName =
        DirectiveHelper.getStringArgument(directive, "name", context).orElse(graphqlName);

    Target target =
        DirectiveHelper.getEnumArgument(directive, "target", Target.class, context)
            .orElse(Target.TABLE);

    List<FieldMappingModel> partitionKey = new ArrayList<>();
    List<FieldMappingModel> clusteringColumns = new ArrayList<>();
    List<FieldMappingModel> regularColumns = new ArrayList<>();

    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      FieldMappingModel.build(fieldDefinition, context, target)
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

    switch (target) {
      case TABLE:
        if (partitionKey.isEmpty()) {
          context.addError(
              type.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "Objects that map to a table must have at least one partition key field "
                  + "(use scalar type ID, or annotate your fields with @cqlColumn(partitionKey: true))");
          return Optional.empty();
        }
        break;
      case UDT:
        if (regularColumns.isEmpty()) {
          context.addError(
              type.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "Objects that map to a UDT must have at least one field");
        }
        break;
    }

    return Optional.of(
        new EntityMappingModel(
            graphqlName,
            context.getKeyspace().name(),
            cqlName,
            target,
            partitionKey,
            clusteringColumns,
            regularColumns));
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
}
