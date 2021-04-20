package io.stargate.graphql.schema.schemafirst.processor;

import com.google.common.collect.ImmutableList;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.db.query.Predicate;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.EntityListReturnType;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.EntityReturnType;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    Optional<Directive> whereDirective = DirectiveHelper.getDirective("cql_where", inputValue);
    String fieldName =
        whereDirective
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "field", context))
            .orElse(inputValue.getName());
    Predicate predicate =
        whereDirective
            .flatMap(d -> DirectiveHelper.getEnumArgument(d, "predicate", Predicate.class, context))
            .orElse(Predicate.EQ);
    if (predicate == Predicate.NEQ) {
      invalidMapping(
          "Operation %s: predicate NEQ (on %s) is not allowed for WHERE conditions",
          operationName, inputValue.getName());
      throw SkipException.INSTANCE;
    }

    FieldModel field =
        entity.getAllColumns().stream()
            .filter(f -> f.getGraphqlName().equals(fieldName))
            .findFirst()
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Operation %s: could not find field %s in type %s",
                      operationName, fieldName, entity.getGraphqlName());
                  return SkipException.INSTANCE;
                });

    if (!field.isPrimaryKey()) {
      if (!field.getIndex().isPresent()) {
        invalidMapping(
            "Operation %s: non-primary key argument %s must be indexed in order to use it "
                + "in a condition",
            operationName, inputValue.getName());
        throw SkipException.INSTANCE;
      }
      IndexModel index = field.getIndex().get();
      // Only do these checks for regular indexes, because we can't assume what custom indexes
      // support
      if (!index.isCustom()) {
        if (predicate == Predicate.CONTAINS && !field.getCqlType().isCollection()) {
          invalidMapping(
              "Operation %s: CONTAINS predicate cannot be used with argument %s "
                  + "because it is not a collection",
              operationName, inputValue.getName());
          throw SkipException.INSTANCE;
        }
        if (predicate == Predicate.CONTAINS_KEY && !field.getCqlType().isMap()) {
          invalidMapping(
              "Operation %s: CONTAINS_KEY predicate cannot be used with argument %s "
                  + "because it is not a map",
              operationName, inputValue.getName());
          throw SkipException.INSTANCE;
        }
      }
    }

    Type<?> inputType = TypeHelper.unwrapNonNull(inputValue.getType());
    Type<?> fieldType = TypeHelper.unwrapNonNull(field.getGraphqlType());
    switch (predicate) {
      case EQ:
      case LT:
      case GT:
      case LTE:
      case GTE:
        if (!inputType.isEqualTo(fieldType)) {
          invalidMapping(
              "Operation %s: expected argument %s to have type %s to match %s.%s",
              operationName,
              inputValue.getName(),
              TypeHelper.format(fieldType),
              entity.getGraphqlName(),
              field.getGraphqlName());
          throw SkipException.INSTANCE;
        }
        break;
      case IN:
        if (!isListOf(fieldType, inputType)) {
          invalidMapping(
              "Operation %s: expected argument %s to have type [%s] to match %s.%s",
              operationName,
              inputValue.getName(),
              TypeHelper.format(fieldType),
              entity.getGraphqlName(),
              field.getGraphqlName());
        }
        break;
      case CONTAINS:
        if (!isListOf(inputType, fieldType)) {
          invalidMapping(
              "Operation %s: expected argument %s to match element type of %s.%s (%s)",
              operationName,
              inputValue.getName(),
              entity.getGraphqlName(),
              field.getGraphqlName(),
              TypeHelper.format(fieldType));
        }
        break;
      case CONTAINS_KEY:
        throw new IllegalArgumentException("Unsupported predicate " + predicate);
    }

    return new WhereConditionModel(field, predicate, inputValue.getName());
  }

  private boolean isListOf(Type<?> expectedElement, Type<?> expectedList) {
    assert !(expectedElement instanceof NonNullType); // we already unwrapped it in the caller
    if (expectedList instanceof ListType) {
      ListType list = (ListType) expectedList;
      Type<?> actualElement = TypeHelper.unwrapNonNull(list.getType());
      return actualElement.isEqualTo(expectedElement);
    }
    return false;
  }

  protected void validateWhereConditions(
      List<WhereConditionModel> whereConditions, EntityModel entity) throws SkipException {
    Optional<String> maybeError = entity.validateConditions(whereConditions);
    if (maybeError.isPresent()) {
      invalidMapping("Operation %s: %s", operationName, maybeError.get());
      throw SkipException.INSTANCE;
    }
  }
}
