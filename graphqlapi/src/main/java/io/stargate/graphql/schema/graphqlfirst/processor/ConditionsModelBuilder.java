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

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import java.util.List;
import java.util.function.BiFunction;

public class ConditionsModelBuilder {
  public static final String CQL_WHERE = "cql_where";
  public static final String CQL_IF = "cql_if";
  private final BiFunction<InputValueDefinition, EntityModel, ModelBuilderBase<ConditionModel>>
      whereConditionModelBuilder;
  private final BiFunction<InputValueDefinition, EntityModel, ModelBuilderBase<ConditionModel>>
      ifConditionModelBuilder;
  private final FieldDefinition operation;
  private final InvalidMappingReporter invalidMappingReporter;

  public ConditionsModelBuilder(
      BiFunction<InputValueDefinition, EntityModel, ModelBuilderBase<ConditionModel>>
          whereConditionModelBuilder,
      BiFunction<InputValueDefinition, EntityModel, ModelBuilderBase<ConditionModel>>
          ifConditionModelBuilder,
      FieldDefinition operation,
      InvalidMappingReporter invalidMappingReporter) {
    this.whereConditionModelBuilder = whereConditionModelBuilder;
    this.ifConditionModelBuilder = ifConditionModelBuilder;
    this.operation = operation;
    this.invalidMappingReporter = invalidMappingReporter;
  }

  public Conditions build(EntityModel entity) throws SkipException {
    ImmutableList.Builder<ConditionModel> ifConditionsBuilder = ImmutableList.builder();
    ImmutableList.Builder<ConditionModel> whereConditionsBuilder = ImmutableList.builder();
    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {

      if (isCqlIfDirective(inputValue) && isCqlWhereDirective(inputValue)) {
        invalidMappingReporter.invalidMapping(
            "You cannot set both: %s and %s directives on the same field: %s",
            CQL_IF, CQL_IF, inputValue.getName());
        foundErrors = true;
      }

      if (DirectiveHelper.getDirective("cql_pagingState", inputValue).isPresent()) {
        continue;
      }
      try {
        // only fields explicitly annotated with cql_if are used
        if (isCqlIfDirective(inputValue)) {
          ifConditionsBuilder.add(ifConditionModelBuilder.apply(inputValue, entity).build());
        } else {
          // all other fields should be used for cql_where
          whereConditionsBuilder.add(whereConditionModelBuilder.apply(inputValue, entity).build());
        }
      } catch (SkipException __) {
        foundErrors = true;
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return new Conditions(ifConditionsBuilder.build(), whereConditionsBuilder.build());
  }

  private boolean isCqlWhereDirective(InputValueDefinition inputValue) {
    return DirectiveHelper.getDirective(CQL_WHERE, inputValue).isPresent();
  }

  private boolean isCqlIfDirective(InputValueDefinition inputValue) {
    return DirectiveHelper.getDirective(CQL_IF, inputValue).isPresent();
  }

  public static class Conditions {
    private final List<ConditionModel> ifConditions;
    private final List<ConditionModel> whereConditions;

    public Conditions(List<ConditionModel> ifConditions, List<ConditionModel> whereConditions) {
      this.ifConditions = ifConditions;
      this.whereConditions = whereConditions;
    }

    public List<ConditionModel> getIfConditions() {
      return ifConditions;
    }

    public List<ConditionModel> getWhereConditions() {
      return whereConditions;
    }
  }
}
