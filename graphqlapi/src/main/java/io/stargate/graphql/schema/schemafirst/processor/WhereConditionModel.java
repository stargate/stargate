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

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;

/** Maps a GraphQL operation argument to a CQL WHERE clause. */
public class WhereConditionModel {

  private final FieldModel field;
  private final Predicate predicate;
  private final String argumentName;

  public WhereConditionModel(FieldModel field, Predicate predicate, String argumentName) {
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
    // TODO change this when we support operators other than EQ
    // The user will specify which operator to use via a directive, and we'll apply it here.
    return BuiltCondition.of(field.getCqlName(), predicate, cqlValue);
  }
}
