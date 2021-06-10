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

    validate(field, prepend);
    return new IncrementModel(field, prepend, inputValue.getName());
  }

  protected void validate(FieldModel field, boolean prepend) throws SkipException {
    if (field.isPartitionKey() || field.isClusteringColumn()) {
      invalidMapping(
          "Operation %s: directive %s is not supported for partition/clustering key field %s.",
          operationName, CQL_INCREMENT, field.getGraphqlName());
      throw SkipException.INSTANCE;
    } else {
      checkValidForRegularColumn(field, prepend);
    }
  }

  private void checkValidForRegularColumn(FieldModel field, boolean prepend) throws SkipException {
    if (prepend) {
      checkArgumentIsAList(field);
    }
  }

  private void checkArgumentIsAList(FieldModel field) throws SkipException {
    Type<?> fieldInputType =
        toInput(field.getGraphqlType(), inputValue, entity, field, entities, operationName);
    if (!(fieldInputType instanceof ListType)) {
      invalidMapping(
          "Operation %s: the %s directive with prepend = true cannot be used with argument %s "
              + "because it is not a list",
          operationName, CQL_INCREMENT, inputValue.getName());
      throw SkipException.INSTANCE;
    }
  }
}
