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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

class WhereConditionModelBuilder extends ConditionModelBuilderBase {

  WhereConditionModelBuilder(
      InputValueDefinition argument,
      Optional<Directive> directive,
      EntityModel entity,
      FieldModel field,
      String operationName,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(argument, directive, entity, field, operationName, entities, context);
  }

  @Override
  protected void validate(FieldModel field, Predicate predicate) throws SkipException {
    // Check that the predicate is allowed for this type of field, and that the types match:
    if (field.isPartitionKey()) {
      checkValidForPartitionKey(predicate, field);
    } else if (field.isClusteringColumn()) {
      checkValidForClusteringColumn(predicate, field);
    } else {
      checkValidForRegularColumn(predicate, field);
    }
  }

  private void checkValidForPartitionKey(Predicate predicate, FieldModel field)
      throws SkipException {
    switch (predicate) {
      case EQ:
        checkArgumentIsSameAs(field);
        break;
      case IN:
        checkArgumentIsListOf(field);
        break;
      default:
        invalidMapping(
            "Operation %s: predicate %s is not supported for partition key field %s "
                + "(expected EQ or IN)",
            operationName, predicate, field.getGraphqlName());
        throw SkipException.INSTANCE;
    }
  }

  private void checkValidForClusteringColumn(Predicate predicate, FieldModel field)
      throws SkipException {
    switch (predicate) {
      case EQ:
      case LT:
      case GT:
      case LTE:
      case GTE:
        checkArgumentIsSameAs(field);
        break;
      case IN:
        checkArgumentIsListOf(field);
        break;
      default:
        invalidMapping(
            "Operation %s: predicate %s is not supported for clustering field %s "
                + "(expected EQ, LT, GT, LTE, GTE or IN)",
            operationName, predicate, field.getGraphqlName());
        throw SkipException.INSTANCE;
    }
  }

  private void checkValidForRegularColumn(Predicate predicate, FieldModel field)
      throws SkipException {
    IndexModel index =
        field
            .getIndex()
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Operation %s: non-primary key argument %s must be indexed in order to "
                          + "use it in a condition",
                      operationName, argument.getName());
                  return SkipException.INSTANCE;
                });
    // Only perform these checks for known implementations, because we can't assume what custom
    // indexes support
    if (index.isBuiltIn()) {
      switch (predicate) {
        case EQ:
          checkArgumentIsSameAs(field);
          break;
        case IN:
          checkArgumentIsListOf(field);
          break;
        case CONTAINS:
          checkArgumentIsElementOf(field);
          break;
        default:
          invalidMapping(
              "Operation %s: predicate %s is not supported for indexed field %s "
                  + "(expected EQ, IN or CONTAINS)",
              operationName, predicate, field.getGraphqlName());
          throw SkipException.INSTANCE;
      }
    }
  }

  private void checkArgumentIsElementOf(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType = fieldInputType();
    if (!(fieldInputType instanceof ListType)) {
      invalidMapping(
          "Operation %s: CONTAINS predicate cannot be used with argument %s "
              + "because it is not a list",
          operationName, argument.getName());
      throw SkipException.INSTANCE;
    }
    Type<?> expectedArgumentType = ((ListType) fieldInputType).getType();

    if (!TypeHelper.deepEquals(argumentType, expectedArgumentType)) {
      invalidMapping(
          "Operation %s: expected argument %s to have type %s to match element type of %s.%s",
          operationName,
          argument.getName(),
          TypeHelper.format(expectedArgumentType),
          entity.getGraphqlName(),
          field.getGraphqlName());
    }
  }
}
