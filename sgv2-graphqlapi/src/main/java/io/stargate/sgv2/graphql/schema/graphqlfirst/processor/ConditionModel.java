package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;

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

  public BuiltCondition build(Value cqlValue) {
    return BuiltCondition.of(field.getCqlName(), predicate, cqlValue);
  }
}
