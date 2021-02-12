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
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Order;
import java.util.Optional;

public class FieldMappingModel {

  private final String graphqlName;
  private final Type<?> graphqlType;
  private final String cqlName;
  private final Column.ColumnType cqlType;
  private final boolean partitionKey;
  private final Optional<Order> clusteringOrder;

  FieldMappingModel(
      String graphqlName,
      Type<?> graphqlType,
      String cqlName,
      Column.ColumnType cqlType,
      boolean partitionKey,
      Optional<Order> clusteringOrder) {
    this.graphqlName = graphqlName;
    this.graphqlType = graphqlType;
    this.cqlName = cqlName;
    this.cqlType = cqlType;
    this.partitionKey = partitionKey;
    this.clusteringOrder = clusteringOrder;
  }

  public String getGraphqlName() {
    return graphqlName;
  }

  public Type<?> getGraphqlType() {
    return graphqlType;
  }

  public String getCqlName() {
    return cqlName;
  }

  public Column.ColumnType getCqlType() {
    return cqlType;
  }

  public boolean isPartitionKey() {
    return partitionKey;
  }

  public FieldMappingModel asPartitionKey() {
    return new FieldMappingModel(graphqlName, graphqlType, cqlName, cqlType, true, clusteringOrder);
  }

  public Optional<Order> getClusteringOrder() {
    return clusteringOrder;
  }

  public boolean isPrimaryKey() {
    return partitionKey || clusteringOrder.isPresent();
  }

  static Optional<FieldMappingModel> build(
      FieldDefinition field,
      ProcessingContext context,
      String parentName,
      EntityMappingModel.Target targetContainer) {
    String graphqlName = field.getName();
    Type<?> graphqlType = field.getType();

    Optional<Directive> directive = DirectiveHelper.getDirective("cql_column", field);
    String cqlName =
        directive
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "name", context))
            .orElse(graphqlName);
    boolean partitionKey =
        directive
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "partitionKey", context))
            .orElse(false);
    Optional<Order> clusteringOrder =
        directive.flatMap(
            d -> DirectiveHelper.getEnumArgument(d, "clusteringOrder", Order.class, context));

    if (targetContainer == EntityMappingModel.Target.UDT) {
      if (partitionKey) {
        context.addWarning(
            directive.map(Directive::getSourceLocation).orElse(null),
            "%s.%s: this shouldn't be marked as a partition key, the object maps to a UDT "
                + "(this will be ignored)",
            parentName,
            graphqlName);
      }
      if (clusteringOrder.isPresent()) {
        context.addWarning(
            directive.map(Directive::getSourceLocation).orElse(null),
            "%s.%s: this shouldn't be marked as a clustering key, the object maps to a UDT "
                + "(this will be ignored)",
            parentName,
            graphqlName);
      }
    }

    // If the CQL type is explicitly provided, use that, otherwise use the best equivalent of the
    // GraphQL type.
    // TODO if the CQL type is provided, check that we know how to coerce to it
    // TODO we should also consider the case where the table already exists
    Optional<Column.ColumnType> cqlType =
        directive
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "type", context))
            .flatMap(spec -> parseCqlType(spec, context, directive, parentName, graphqlName));
    if (!cqlType.isPresent()) {
      cqlType = guessCqlType(graphqlType, context);
    }

    if (partitionKey && clusteringOrder.isPresent()) {
      context.addError(
          directive.map(Directive::getSourceLocation).orElse(null),
          ProcessingMessageType.InvalidMapping,
          "%s.%s: can't be both a partition key and a clustering key.",
          parentName,
          graphqlName);
      return Optional.empty();
    }

    return cqlType.map(
        t ->
            new FieldMappingModel(
                graphqlName, graphqlType, cqlName, t, partitionKey, clusteringOrder));
  }

  private static Optional<Column.ColumnType> parseCqlType(
      String spec,
      ProcessingContext context,
      Optional<Directive> directive,
      String parentName,
      String graphqlName) {
    try {
      return Optional.of(Column.Type.fromCqlDefinitionOf(context.getKeyspace(), spec));
    } catch (IllegalArgumentException e) {
      context.addError(
          directive.map(Directive::getSourceLocation).orElse(null),
          ProcessingMessageType.InvalidSyntax,
          "%s.%s: could not parse CQL type '%s': %s",
          parentName,
          graphqlName,
          spec,
          e.getMessage());
      return Optional.empty();
    }
  }

  private static Optional<Column.ColumnType> guessCqlType(
      Type<?> graphqlType, ProcessingContext context) {
    if (graphqlType instanceof NonNullType) {
      return guessCqlType(((NonNullType) graphqlType).getType(), context);
    } else if (graphqlType instanceof ListType) {
      return guessCqlType(((ListType) graphqlType).getType(), context).map(Column.Type.List::of);
    } else {
      String typeName = ((TypeName) graphqlType).getName();
      switch (typeName) {
          // Built-in scalars:
        case "Int":
          return Optional.of(Column.Type.Int);
        case "Float":
          return Optional.of(Column.Type.Float);
        case "String":
          return Optional.of(Column.Type.Varchar);
        case "Boolean":
          return Optional.of(Column.Type.Boolean);
        case "ID":
          return Optional.of(Column.Type.Uuid);
        default:
          // Try to fall back to an enum type
          return context
              .getTypeRegistry()
              .getType(typeName)
              .filter(t -> t instanceof EnumTypeDefinition)
              .map(__ -> Column.Type.Varchar);
          // TODO also allow other object type if it maps to a UDT
      }
    }
  }
}
