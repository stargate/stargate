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

import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import io.stargate.db.query.Predicate;
import java.util.Map;
import java.util.Optional;

class IfConditionModelBuilder extends ConditionModelBuilderBase {

  IfConditionModelBuilder(
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
    // The CQL IF works only for regular columns (non PK, CK)
    if (field.isPartitionKey()) {
      invalidMapping(
          "Operation %s: @%s is not supported for partition keys (field %s)",
          operationName, CqlDirectives.IF, field.getGraphqlName());
    }
    if (field.isClusteringColumn()) {
      invalidMapping(
          "Operation %s: @%s is not supported for clustering keys (field %s)",
          operationName, CqlDirectives.IF, field.getGraphqlName());
    } else {
      checkValidForRegularColumn(predicate, field);
    }
  }

  private void checkValidForRegularColumn(Predicate predicate, FieldModel field)
      throws SkipException {
    switch (predicate) {
      case EQ:
      case NEQ:
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
            "Operation %s: predicate %s is not supported for field %s "
                + "(expected EQ, NEQ, LT, GT, LTE or GTE)",
            operationName, predicate, field.getGraphqlName());
        throw SkipException.INSTANCE;
    }
  }
}
