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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.StringValue;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.proto.Schema.ColumnOrderBy;
import io.stargate.proto.Schema.CqlIndex;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
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

    CqlTable tableCqlSchema;
    Udt udtCqlSchema;
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
        tableCqlSchema = buildCqlTable(cqlName, partitionKey, clusteringColumns, regularColumns);
        udtCqlSchema = null;
        break;
      case UDT:
        if (regularColumns.isEmpty()) {
          invalidMapping("%s must have at least one field", graphqlName);
          throw SkipException.INSTANCE;
        }
        tableCqlSchema = null;
        udtCqlSchema = buildCqlUdt(cqlName, regularColumns);
        break;
      default:
        throw new AssertionError("Unexpected target " + target);
    }

    return new EntityModel(
        graphqlName,
        context.getKeyspace().getCqlKeyspace().getName(),
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

  private static CqlTable buildCqlTable(
      String tableName,
      List<FieldModel> partitionKey,
      List<FieldModel> clusteringColumns,
      List<FieldModel> regularColumns) {

    CqlTable.Builder table = CqlTable.newBuilder().setName(tableName);

    for (FieldModel column : partitionKey) {
      table.addPartitionKeyColumns(
          ColumnSpec.newBuilder().setName(column.getCqlName()).setType(column.getCqlType()));
    }

    for (FieldModel column : clusteringColumns) {
      table.addClusteringKeyColumns(
          ColumnSpec.newBuilder().setName(column.getCqlName()).setType(column.getCqlType()));
      table.putClusteringOrders(
          column.getCqlName(), column.getClusteringOrder().orElse(ColumnOrderBy.ASC));
    }

    for (FieldModel column : regularColumns) {
      table.addColumns(
          ColumnSpec.newBuilder().setName(column.getCqlName()).setType(column.getCqlType()));
      column
          .getIndex()
          .ifPresent(
              index -> {
                CqlIndex.Builder builder =
                    CqlIndex.newBuilder()
                        .setColumnName(column.getCqlName())
                        .setName(index.getName())
                        .setIndexingType(index.getIndexingType())
                        .putAllOptions(index.getOptions());
                index.getIndexClass().ifPresent(c -> builder.setIndexingClass(StringValue.of(c)));
                table.addIndexes(builder);
              });
    }

    return table.build();
  }

  private static Udt buildCqlUdt(String udtName, List<FieldModel> fields) {
    Udt.Builder udt = Udt.newBuilder().setName(udtName);
    for (FieldModel field : fields) {
      udt.putFields(field.getCqlName(), field.getCqlType());
    }
    return udt.build();
  }
}
