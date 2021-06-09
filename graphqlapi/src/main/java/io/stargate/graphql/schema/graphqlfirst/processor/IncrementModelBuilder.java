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

import graphql.language.*;
import java.util.*;

public class IncrementModelBuilder extends ModelBuilderBase<IncrementModel> {

  private final EntityModel entity;
  private final FieldModel field;
  private final Map<String, EntityModel> entities;
  private final ProcessingContext context;

  private static final boolean PREPEND_DEFAULT = false;
  public static final String CQL_INCREMENT = "cql_increment";
  private final String operationName;
  private final InputValueDefinition inputValue;

  public IncrementModelBuilder(
      InputValueDefinition inputValue,
      String operationName,
      EntityModel entity,
      FieldModel field,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(context, inputValue.getSourceLocation());
    this.inputValue = inputValue;
    this.operationName = operationName;
    this.entity = entity;
    this.field = field;
    this.entities = entities;
    this.context = context;
  }

  @Override
  IncrementModel build() throws SkipException {

    Optional<Directive> directive = DirectiveHelper.getDirective(CQL_INCREMENT, inputValue);

    boolean prepend =
        directive
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "prepend", context))
            .orElse(PREPEND_DEFAULT);

    return new IncrementModel(field, prepend, inputValue.getName());
  }
}
