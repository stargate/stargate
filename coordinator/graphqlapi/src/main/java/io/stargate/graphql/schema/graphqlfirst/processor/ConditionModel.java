package io.stargate.graphql.schema.graphqlfirst.processor;

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;

public class ConditionModel {

  private final FieldModel field;
  private final Predicate predicate;
  private final String argumentName;

  public ConditionModel(FieldModel field, Predicate predicate, String argumentName) {
    this.field = field;
    this.predicate = predicate;
    this.argumentName = argumentName;
  }

  /** The entity field that the condition applies to. */
  public FieldModel getField() {
    return field;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  /** The name of the argument that will contain the value to bind. */
  public String getArgumentName() {
    return argumentName;
  }

  public BuiltCondition build(Object cqlValue) {
    return BuiltCondition.of(field.getCqlName(), predicate, cqlValue);
  }
}
