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

/**
 * Turns a GraphQL operation argument into a WHERE clause that will be added to the generated query.
 */
public class WhereConditionModel {

  private final FieldModel field;

  public WhereConditionModel(FieldModel field) {
    this.field = field;
  }

  /** The entity field that the condition applies to. */
  public FieldModel getField() {
    return field;
  }

  public BuiltCondition build(Object cqlValue) {
    // TODO change this when we support operators other than EQ
    // The user will specify which operator to use via a directive, and we'll apply it here.
    return BuiltCondition.of(field.getCqlName(), Predicate.EQ, cqlValue);
  }
}
