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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityMappingModel {
  private static final Pattern NON_NESTED_FIELDS =
      Pattern.compile("[_A-Za-z][_0-9A-Za-z]*(?:\\s+[_A-Za-z][_0-9A-Za-z]*)*");
  private static final Splitter ON_SPACES = Splitter.onPattern("\\s+");

  private final String graphqlName;
  private final String keyspaceName;
  private final String cqlName;
  private final Target target;
  private final List<FieldMappingModel> partitionKey;
  private final List<FieldMappingModel> clusteringColumns;
  private final List<FieldMappingModel> primaryKey;
  private final List<FieldMappingModel> regularColumns;
  private final List<FieldMappingModel> allColumns;
  private final Table tableCqlSchema;
  private final UserDefinedType udtCqlSchema;
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
      Table tableCqlSchema,
      UserDefinedType udtCqlSchema,
      boolean isFederated,
      Optional<String> inputTypeName) {

    assert (target == Target.TABLE && tableCqlSchema != null && udtCqlSchema == null)
        || (target == Target.UDT && tableCqlSchema == null && udtCqlSchema != null);

    this.graphqlName = graphqlName;
    this.keyspaceName = keyspaceName;
    this.cqlName = cqlName;
    this.target = target;
    this.partitionKey = ImmutableList.copyOf(partitionKey);
    this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
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
    this.tableCqlSchema = tableCqlSchema;
    this.udtCqlSchema = udtCqlSchema;
    this.isFederated = isFederated;
    this.inputTypeName = inputTypeName;
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

  public Table getTableCqlSchema() {
    if (target != Target.TABLE) {
      throw new UnsupportedOperationException("Can't call this method when target = " + target);
    }
    return tableCqlSchema;
  }

  public UserDefinedType getUdtCqlSchema() {
    if (target != Target.UDT) {
      throw new UnsupportedOperationException("Can't call this method when target = " + target);
    }
    return udtCqlSchema;
  }

  public boolean isFederated() {
    return isFederated;
  }

  public Optional<String> getInputTypeName() {
    return inputTypeName;
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

    List<FieldMappingModel> partitionKey = new ArrayList<>();
    List<FieldMappingModel> clusteringColumns = new ArrayList<>();
    List<FieldMappingModel> regularColumns = new ArrayList<>();

    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      new FieldMappingModelBuilder(
              fieldDefinition, context, graphqlName, target, inputTypeName.isPresent())
          .build()
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

    Table tableCqlSchema;
    UserDefinedType udtCqlSchema;
    // Check that we have the necessary kinds of columns depending on the target:
    switch (target) {
      case TABLE:
        if (partitionKey.isEmpty()) {
          FieldMappingModel firstField = regularColumns.get(0);
          if (TypeHelper.mapsToUuid(firstField.getGraphqlType())) {
            context.addInfo(
                type.getSourceLocation(),
                "%s: using %s as the partition key, "
                    + "because it uses type %s and no other fields are annotated",
                graphqlName,
                firstField.getGraphqlName(),
                TypeHelper.format(TypeHelper.unwrapNonNull(firstField.getGraphqlType())));
            partitionKey.add(firstField.asPartitionKey());
            regularColumns.remove(firstField);
          } else {
            context.addError(
                type.getSourceLocation(),
                ProcessingErrorType.InvalidMapping,
                "%s must have at least one partition key field "
                    + "(use scalar type ID, Uuid or TimeUuid, "
                    + "or annotate your fields with @cql_column(partitionKey: true))",
                graphqlName);
            return Optional.empty();
          }
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
          context.addError(
              type.getSourceLocation(),
              ProcessingErrorType.InvalidMapping,
              "%s must have at least one field",
              graphqlName);
          return Optional.empty();
        }
        tableCqlSchema = null;
        udtCqlSchema = buildCqlUdt(context.getKeyspace().name(), cqlName, regularColumns);
        break;
      default:
        throw new AssertionError("Unexpected target " + target);
    }

    // Check that if the @key directive is present, it matches the CQL primary key:
    List<Directive> keyDirectives = type.getDirectives("key");
    boolean isFederated;
    if (!keyDirectives.isEmpty()) {
      if (target == Target.UDT) {
        context.addError(
            type.getSourceLocation(),
            ProcessingErrorType.InvalidMapping,
            "%s: can't use @key directive because this type maps to a UDT",
            graphqlName);
        return Optional.empty();
      }
      if (keyDirectives.size() > 1) {
        context.addError(
            type.getSourceLocation(),
            ProcessingErrorType.InvalidMapping,
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
              ProcessingErrorType.InvalidMapping,
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
              ProcessingErrorType.InvalidMapping,
              "%s: @key.fields doesn't match the partition and clustering keys (expected %s)",
              graphqlName,
              primaryKeyFields);
          return Optional.empty();
        }
        isFederated = true;
      } else {
        context.addError(
            keyDirective.getSourceLocation(),
            ProcessingErrorType.InvalidSyntax,
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
            tableCqlSchema,
            udtCqlSchema,
            isFederated,
            inputTypeName));
  }

  private static Table buildCqlTable(
      String keyspaceName,
      String tableName,
      List<FieldMappingModel> partitionKey,
      List<FieldMappingModel> clusteringColumns,
      List<FieldMappingModel> regularColumns) {
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
      String keyspaceName, String tableName, List<FieldMappingModel> regularColumns) {
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
      String keyspaceName, String cqlName, FieldMappingModel field) {
    return ImmutableColumn.builder()
        .keyspace(keyspaceName)
        .table(cqlName)
        .name(field.getCqlName())
        .type(field.getCqlType());
  }

  public enum Target {
    TABLE,
    UDT,
    ;
  }
}
