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

class FieldMappingModel {

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

  String getGraphqlName() {
    return graphqlName;
  }

  Type<?> getGraphqlType() {
    return graphqlType;
  }

  String getCqlName() {
    return cqlName;
  }

  Column.ColumnType getCqlType() {
    return cqlType;
  }

  boolean isPartitionKey() {
    return partitionKey;
  }

  Optional<Order> getClusteringOrder() {
    return clusteringOrder;
  }

  static Optional<FieldMappingModel> build(
      FieldDefinition field, ProcessingContext context, EntityMappingModel.Target targetContainer) {
    String graphqlName = field.getName();
    Type<?> graphqlType = field.getType();

    Optional<Directive> directive = DirectiveHelper.getDirective("cqlColumn", field);
    String cqlName =
        DirectiveHelper.getStringArgument(directive, "name", context).orElse(graphqlName);
    boolean partitionKey =
        DirectiveHelper.getBooleanArgument(directive, "partitionKey", context)
            .orElse(targetContainer == EntityMappingModel.Target.TABLE && isGraphqlId(graphqlType));
    Optional<Order> clusteringOrder =
        DirectiveHelper.getEnumArgument(directive, "clusteringOrder", Order.class, context);

    if (targetContainer == EntityMappingModel.Target.UDT) {
      if (partitionKey) {
        context.addWarning(
            directive.map(Directive::getSourceLocation).orElse(null),
            "Objects that map to a UDT should not have partition key fields "
                + "(this will be ignored)");
      }
      if (clusteringOrder.isPresent()) {
        context.addWarning(
            directive.map(Directive::getSourceLocation).orElse(null),
            "Objects that map to a UDT should not have clustering fields"
                + "(this will be ignored)");
      }
    }

    // If the CQL type is explicitly provided, use that, otherwise use the best equivalent of the
    // GraphQL type.
    // TODO we should also consider the case where the table already exists
    Optional<Column.ColumnType> cqlType =
        DirectiveHelper.getStringArgument(directive, "type", context)
            .flatMap(spec -> parseCqlType(spec, context, directive));
    if (!cqlType.isPresent()) {
      cqlType = guessCqlType(graphqlType, context);
    }

    if (partitionKey && clusteringOrder.isPresent()) {
      context.addError(
          directive.map(Directive::getSourceLocation).orElse(null),
          ProcessingMessageType.InvalidMapping,
          "A field can't be both a partition key component and a clustering column.");
      return Optional.empty();
    }

    return cqlType.map(
        t ->
            new FieldMappingModel(
                graphqlName, graphqlType, cqlName, t, partitionKey, clusteringOrder));
  }

  private static boolean isGraphqlId(Type<?> type) {
    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }
    return type instanceof TypeName && ((TypeName) type).getName().equals("ID");
  }

  private static Optional<Column.ColumnType> parseCqlType(
      String spec, ProcessingContext context, Optional<Directive> directive) {
    try {
      return Optional.of(Column.Type.fromCqlDefinitionOf(context.getKeyspace(), spec));
    } catch (IllegalArgumentException e) {
      context.addError(
          directive.map(Directive::getSourceLocation).orElse(null),
          ProcessingMessageType.InvalidSyntax,
          "Could not parse CQL type from '%s': %s",
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
