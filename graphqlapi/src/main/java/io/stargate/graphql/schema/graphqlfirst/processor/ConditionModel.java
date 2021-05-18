package io.stargate.graphql.schema.graphqlfirst.processor;

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;

public interface ConditionModel {
  FieldModel getField();;

  Predicate getPredicate();

  /** The name of the argument that will contain the value to bind. */
  String getArgumentName();

  BuiltCondition build(Object cqlValue);
}
