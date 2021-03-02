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

import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityModelBuilder extends ModelBuilderBase<EntityModel> {

  private static final Logger LOG = LoggerFactory.getLogger(EntityModelBuilder.class);

  private final ObjectTypeDefinition type;
  private final String graphqlName;

  EntityModelBuilder(ObjectTypeDefinition type, ProcessingContext context) {
    super(context, type.getSourceLocation());
    this.type = type;
    this.graphqlName = type.getName();
  }

  @Override
  EntityModel build() throws SkipException {
    Optional<Directive> cqlEntityDirective = DirectiveHelper.getDirective("cql_entity", type);
    String cqlName = providedCqlNameOrDefault(cqlEntityDirective);
    EntityModel.Target target = providedTargetOrDefault(cqlEntityDirective);
    Optional<String> inputTypeName =
        DirectiveHelper.getDirective("cql_input", type).map(this::providedInputNameOrDefault);

    List<FieldModel> partitionKey = new ArrayList<>();
    List<FieldModel> clusteringColumns = new ArrayList<>();
    List<FieldModel> regularColumns = new ArrayList<>();

    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      try {
        FieldModel fieldMapping =
            new FieldModelBuilder(
                    fieldDefinition, context, graphqlName, target, inputTypeName.isPresent())
                .build();
        if (fieldMapping.isPartitionKey()) {
          partitionKey.add(fieldMapping);
        } else if (fieldMapping.getClusteringOrder().isPresent()) {
          clusteringColumns.add(fieldMapping);
        } else {
          regularColumns.add(fieldMapping);
        }
      } catch (SkipException e) {
        LOG.debug(
            "Skipping field {} because it has mapping errors, "
                + "this will be reported after the whole schema has been processed.",
            fieldDefinition.getName());
      }
    }

    Table tableCqlSchema;
    UserDefinedType udtCqlSchema;
    // Check that we have the necessary kinds of columns depending on the target:
    switch (target) {
      case TABLE:
        if (!hasPartitionKey(partitionKey, regularColumns)) {
          invalidMapping(
              "%s must have at least one partition key field "
                  + "(use scalar type ID, Uuid or TimeUuid for the first field, "
                  + "or annotate your fields with @cql_column(partitionKey: true))",
              graphqlName);
          throw SkipException.INSTANCE;
        }
        tableCqlSchema =
            buildCqlTable(
                context.getKeyspace().name(),
                cqlName,
                partitionKey,
                clusteringColumns,
                regularColumns);
        udtCqlSchema = null;
        break;
      case UDT:
        if (regularColumns.isEmpty()) {
          invalidMapping("%s must have at least one field", graphqlName);
          throw SkipException.INSTANCE;
        }
        tableCqlSchema = null;
        udtCqlSchema = buildCqlUdt(context.getKeyspace().name(), cqlName, regularColumns);
        break;
      default:
        throw new AssertionError("Unexpected target " + target);
    }

    return new EntityModel(
        graphqlName,
        context.getKeyspace().name(),
        cqlName,
        target,
        partitionKey,
        clusteringColumns,
        regularColumns,
        tableCqlSchema,
        udtCqlSchema,
        isFederated(partitionKey, clusteringColumns, target),
        inputTypeName);
  }

  private String providedCqlNameOrDefault(Optional<Directive> cqlEntityDirective) {
    return cqlEntityDirective
        .flatMap(d -> DirectiveHelper.getStringArgument(d, "name", context))
        .orElse(graphqlName);
  }

  private EntityModel.Target providedTargetOrDefault(Optional<Directive> cqlEntityDirective) {
    return cqlEntityDirective
        .flatMap(
            d -> DirectiveHelper.getEnumArgument(d, "target", EntityModel.Target.class, context))
        .orElse(EntityModel.Target.TABLE);
  }

  private String providedInputNameOrDefault(Directive cqlInputDirective) {
    Optional<String> maybeName =
        DirectiveHelper.getStringArgument(cqlInputDirective, "name", context);
    if (maybeName.isPresent()) {
      return maybeName.get();
    } else {
      info(
          "%1$s: using '%1$sInput' as the input type name since @cql_input doesn't "
              + "have an argument",
          graphqlName);
      return graphqlName + "Input";
    }
  }

  private boolean hasPartitionKey(List<FieldModel> partitionKey, List<FieldModel> regularColumns) {
    if (!partitionKey.isEmpty()) {
      return true;
    }
    FieldModel firstField = regularColumns.get(0);
    if (TypeHelper.mapsToUuid(firstField.getGraphqlType())) {
      info(
          "%s: using %s as the partition key, "
              + "because it has type %s and no other fields are annotated",
          graphqlName,
          firstField.getGraphqlName(),
          TypeHelper.format(TypeHelper.unwrapNonNull(firstField.getGraphqlType())));
      partitionKey.add(firstField.asPartitionKey());
      regularColumns.remove(firstField);
      return true;
    }
    return false;
  }

  private boolean isFederated(
      List<FieldModel> partitionKey, List<FieldModel> clusteringColumns, EntityModel.Target target)
      throws SkipException {
    List<Directive> keyDirectives = type.getDirectives("key");
    if (keyDirectives.isEmpty()) {
      return false;
    }

    if (target == EntityModel.Target.UDT) {
      invalidMapping("%s: can't use @key directive because this type maps to a UDT", graphqlName);
      throw SkipException.INSTANCE;
    }
    if (keyDirectives.size() > 1) {
      // The spec allows multiple `@key`s, but we don't (yet?)
      invalidMapping("%s: this implementation only supports a single @key directive", graphqlName);
      throw SkipException.INSTANCE;
    }
    Directive keyDirective = keyDirectives.get(0);

    Optional<String> fieldsArgument =
        DirectiveHelper.getStringArgument(keyDirective, "fields", context);
    if (fieldsArgument.isPresent()) {
      Set<String> primaryKeyFields =
          Stream.concat(partitionKey.stream(), clusteringColumns.stream())
              .map(FieldModel::getGraphqlName)
              .collect(Collectors.toSet());
      invalidSyntax(
          "%s: argument '@key.fields' is not supported. Stargate only supports the natural "
              + "primary key (%s), so it will implicitly set those fields, you don't need to "
              + "list them explicitly.",
          graphqlName, primaryKeyFields);
      throw SkipException.INSTANCE;
    }
    return true;
  }

  private static Table buildCqlTable(
      String keyspaceName,
      String tableName,
      List<FieldModel> partitionKey,
      List<FieldModel> clusteringColumns,
      List<FieldModel> regularColumns) {
    return ImmutableTable.builder()
        .keyspace(keyspaceName)
        .name(tableName)
        .addAllColumns(
            partitionKey.stream()
                .map(
                    field ->
                        cqlColumnBuilder(keyspaceName, tableName, field)
                            .kind(Column.Kind.PartitionKey)
                            .build())
                .collect(Collectors.toList()))
        .addAllColumns(
            clusteringColumns.stream()
                .map(
                    field -> {
                      assert field.getClusteringOrder().isPresent();
                      return cqlColumnBuilder(keyspaceName, tableName, field)
                          .kind(Column.Kind.Clustering)
                          .order(field.getClusteringOrder().get())
                          .build();
                    })
                .collect(Collectors.toList()))
        .addAllColumns(
            regularColumns.stream()
                .map(
                    field ->
                        cqlColumnBuilder(keyspaceName, tableName, field)
                            .kind(Column.Kind.Regular)
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private static UserDefinedType buildCqlUdt(
      String keyspaceName, String tableName, List<FieldModel> regularColumns) {
    return ImmutableUserDefinedType.builder()
        .keyspace(keyspaceName)
        .name(tableName)
        .columns(
            regularColumns.stream()
                .map(
                    field ->
                        cqlColumnBuilder(keyspaceName, tableName, field)
                            .kind(Column.Kind.Regular)
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private static ImmutableColumn.Builder cqlColumnBuilder(
      String keyspaceName, String cqlName, FieldModel field) {
    return ImmutableColumn.builder()
        .keyspace(keyspaceName)
        .table(cqlName)
        .name(field.getCqlName())
        .type(field.getCqlType());
  }
}
