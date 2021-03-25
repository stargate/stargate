package io.stargate.graphql.schema.schemafirst.processor;

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.EntityListReturnType;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.EntityReturnType;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class OperationModelBuilderBase<T extends OperationModel> extends ModelBuilderBase<T> {

  protected final FieldDefinition operation;
  protected final String operationName;
  protected final Map<String, EntityModel> entities;
  protected final Map<String, ResponsePayloadModel> responsePayloads;

  protected OperationModelBuilderBase(
      FieldDefinition operation,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(context, operation.getSourceLocation());
    this.operation = operation;
    this.operationName = operation.getName();
    this.entities = entities;
    this.responsePayloads = responsePayloads;
  }

  OperationModel.ReturnType getReturnType(String operationDescription) throws SkipException {
    Type<?> graphqlType = TypeHelper.unwrapNonNull(operation.getType());

    if (graphqlType instanceof ListType) {
      Type<?> elementType = ((ListType) graphqlType).getType();
      elementType = TypeHelper.unwrapNonNull(elementType);
      if (elementType instanceof TypeName) {
        EntityModel entity = entities.get(((TypeName) elementType).getName());
        if (entity != null) {
          return new EntityListReturnType(entity);
        }
      }
    } else {
      assert graphqlType instanceof TypeName;
      String typeName = ((TypeName) graphqlType).getName();

      SimpleReturnType simple = SimpleReturnType.fromTypeName(typeName);
      if (simple != null) {
        return simple;
      }

      EntityModel entity = entities.get(typeName);
      if (entity != null) {
        return new EntityReturnType(entity);
      }

      ResponsePayloadModel payload = responsePayloads.get(typeName);
      if (payload != null) {
        return payload;
      }
    }
    invalidMapping(
        "%s: unsupported return type %s", operationDescription, TypeHelper.format(graphqlType));
    throw SkipException.INSTANCE;
  }

  /**
   * For each field of the given entity, try to find an operation argument of the same name, and
   * build a condition that will get appended to the CQL query.
   */
  protected List<WhereConditionModel> buildWhereConditions(EntityModel entity)
      throws SkipException {
    ImmutableList.Builder<WhereConditionModel> whereConditionsBuilder = ImmutableList.builder();
    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {
      if (DirectiveHelper.getDirective("cql_pagingState", inputValue).isPresent()) {
        continue;
      }
      try {
        whereConditionsBuilder.add(buildWhereCondition(inputValue, entity));
      } catch (SkipException __) {
        foundErrors = true;
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return whereConditionsBuilder.build();
  }

  private WhereConditionModel buildWhereCondition(
      InputValueDefinition inputValue, EntityModel entity) throws SkipException {
    FieldModel field =
        entity.getAllColumns().stream()
            .filter(f -> f.getGraphqlName().equals(inputValue.getName()))
            .findFirst()
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Operation %s: argument %s does not match any field of type %s",
                      operationName, inputValue.getName(), entity.getGraphqlName());
                  return SkipException.INSTANCE;
                });

    // TODO also allow regular columns, if they are indexed
    if (!field.isPrimaryKey()) {
      invalidMapping(
          "Operation %s: argument %s must be a primary key field in type %s",
          operationName, inputValue.getName(), entity.getGraphqlName());
      throw SkipException.INSTANCE;
    }

    Type<?> inputType = inputValue.getType();
    if (!TypeHelper.unwrapNonNull(inputType)
        .isEqualTo(TypeHelper.unwrapNonNull(field.getGraphqlType()))) {
      invalidMapping(
          "Operation %s: expected argument %s to have the same type as %s.%s",
          operationName, inputValue.getName(), entity.getGraphqlName(), field.getGraphqlName());
      throw SkipException.INSTANCE;
    }
    // TODO support operators other than '=' for clustering and regular columns
    return new WhereConditionModel(field);
  }

  /** Validates that the given set of conditions will produce a valid CQL query. */
  protected void validateWhereConditions(
      List<WhereConditionModel> whereConditions, EntityModel entity) throws SkipException {
    // TODO revisit these rules when we allow regular columns and operators other than '='

    for (FieldModel field : entity.getPartitionKey()) {
      if (whereConditions.stream().noneMatch(c -> c.getField().equals(field))) {
        invalidMapping(
            "Operation %s: every partition key field of type %s must be present (expected: %s)",
            operationName,
            entity.getGraphqlName(),
            entity.getPartitionKey().stream()
                .map(FieldModel::getGraphqlName)
                .collect(Collectors.joining(", ")));
        throw SkipException.INSTANCE;
      }
    }

    String lastMissingField = null;
    for (FieldModel field : entity.getClusteringColumns()) {
      if (whereConditions.stream().noneMatch(c -> c.getField().equals(field))) {
        if (lastMissingField == null) {
          lastMissingField = field.getGraphqlName();
        }
      } else {
        if (lastMissingField != null) {
          invalidMapping(
              "Operation %s: unexpected argument %s. Clustering field %s is not an argument, "
                  + "so no other clustering field after it can be either.",
              operationName, field.getGraphqlName(), lastMissingField);
          throw SkipException.INSTANCE;
        }
      }
    }
  }
}
