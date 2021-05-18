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

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;

/** Maps a GraphQL operation argument to a CQL IF clause that is used with DELETE statements. */
public class IfConditionModel implements ConditionModel {

  private final FieldModel field;
  private final Predicate predicate;
  private final String argumentName;

  public IfConditionModel(FieldModel field, Predicate predicate, String argumentName) {
    this.field = field;
    this.predicate = predicate;
    this.argumentName = argumentName;
  }

  /** The entity field that the condition applies to. */
  @Override
  public FieldModel getField() {
    return field;
  }

  @Override
  public Predicate getPredicate() {
    return predicate;
  }

  /** The name of the argument that will contain the value to bind. */
  @Override
  public String getArgumentName() {
    return argumentName;
  }

  @Override
  public BuiltCondition build(Object cqlValue) {
    return BuiltCondition.of(field.getCqlName(), predicate, cqlValue);
  }
}
