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
package io.stargate.graphql.schema.graphqlfirst.processor;

import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableSecondaryIndex;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityModelBuilder extends ModelBuilderBase<EntityModel> {

  private static final Logger LOG = LoggerFactory.getLogger(EntityModelBuilder.class);
  private static final Pattern NON_NESTED_FIELDS =
      Pattern.compile("[_A-Za-z][_0-9A-Za-z]*(?:\\s+[_A-Za-z][_0-9A-Za-z]*)*");
  private static final Splitter ON_SPACES = Splitter.onPattern("\\s+");

  private final ObjectTypeDefinition type;
  private final String graphqlName;

  EntityModelBuilder(ObjectTypeDefinition type, ProcessingContext context) {
    super(context, type.getSourceLocation());
    this.type = type;
    this.graphqlName = type.getName();
  }

  @Override
  EntityModel build() throws SkipException {
    Optional<Directive> cqlEntityDirective =
        DirectiveHelper.getDirective(CqlDirectives.ENTITY, type);
    String cqlName = providedCqlNameOrDefault(cqlEntityDirective);
    EntityModel.Target target = providedTargetOrDefault(cqlEntityDirective);
    Optional<String> inputTypeName =
        DirectiveHelper.getDirective(CqlDirectives.INPUT, type)
            .map(this::providedInputNameOrDefault);

    List<FieldModel> partitionKey = new ArrayList<>();
    List<FieldModel> clusteringColumns = new ArrayList<>();
    List<FieldModel> regularColumns = new ArrayList<>();

    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      try {
        FieldModel fieldMapping =
            new FieldModelBuilder(
                    fieldDefinition,
                    context,
                    cqlName,
                    graphqlName,
                    target,
                    inputTypeName.isPresent())
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
                  + "or annotate your fields with @%s(%s: true))",
              graphqlName, CqlDirectives.COLUMN, CqlDirectives.COLUMN_PARTITION_KEY);
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
        .flatMap(d -> DirectiveHelper.getStringArgument(d, CqlDirectives.ENTITY_NAME, context))
        .orElse(graphqlName);
  }

  private EntityModel.Target providedTargetOrDefault(Optional<Directive> cqlEntityDirective) {
    return cqlEntityDirective
        .flatMap(
            d ->
                DirectiveHelper.getEnumArgument(
                    d, CqlDirectives.ENTITY_TARGET, EntityModel.Target.class, context))
        .orElse(EntityModel.Target.TABLE);
  }

  private String providedInputNameOrDefault(Directive cqlInputDirective) {
    Optional<String> maybeName =
        DirectiveHelper.getStringArgument(cqlInputDirective, CqlDirectives.INPUT_NAME, context);
    if (maybeName.isPresent()) {
      return maybeName.get();
    } else {
      info(
          "%1$s: using '%1$sInput' as the input type name since @%2$s doesn't have an argument",
          graphqlName, CqlDirectives.INPUT);
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

    // If @key.fields is not explicitly set, we'll fill it later (see
    // SchemaProcessor.finalizeKeyDirectives). But if it is present, check that it matches the
    // primary key.
    Optional<String> fieldsArgument =
        DirectiveHelper.getStringArgument(keyDirective, "fields", context);
    if (fieldsArgument.isPresent()) {
      String value = fieldsArgument.get();
      if (!NON_NESTED_FIELDS.matcher(value).matches()) {
        // The spec allows complex fields expressions like `foo.bar`, but we don't (yet?)
        invalidMapping(
            "%s: could not parse @key.fields "
                + "(this implementation only supports top-level fields as key components)",
            graphqlName);
        throw SkipException.INSTANCE;
      }
      Set<String> directiveFields = ImmutableSet.copyOf(ON_SPACES.split(value));
      Set<String> primaryKeyFields =
          Stream.concat(partitionKey.stream(), clusteringColumns.stream())
              .map(FieldModel::getGraphqlName)
              .collect(Collectors.toSet());
      if (!directiveFields.equals(primaryKeyFields)) {
        invalidMapping(
            "%s: @key.fields doesn't match the partition and clustering keys (expected %s)",
            graphqlName, primaryKeyFields);
        throw SkipException.INSTANCE;
      }
    }
    return true;
  }

  private static Table buildCqlTable(
      String keyspaceName,
      String tableName,
      List<FieldModel> partitionKey,
      List<FieldModel> clusteringColumns,
      List<FieldModel> regularColumns) {

    ImmutableList.Builder<Column> columnMetadatas = ImmutableList.builder();
    ImmutableList.Builder<SecondaryIndex> indexes = ImmutableList.builder();
    for (FieldModel field : regularColumns) {
      Column column =
          cqlColumnBuilder(keyspaceName, tableName, field).kind(Column.Kind.Regular).build();
      columnMetadatas.add(column);
      field
          .getIndex()
          .ifPresent(
              index -> {
                ImmutableSecondaryIndex.Builder builder =
                    ImmutableSecondaryIndex.builder()
                        .keyspace(keyspaceName)
                        .column(column)
                        .name(index.getName())
                        .indexingClass(index.getIndexClass().orElse(null))
                        .indexingType(index.getIndexingType())
                        .putAllIndexingOptions(index.getOptions());
                indexes.add(builder.build());
              });
    }

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
        .addAllColumns(columnMetadatas.build())
        .addAllIndexes(indexes.build())
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
