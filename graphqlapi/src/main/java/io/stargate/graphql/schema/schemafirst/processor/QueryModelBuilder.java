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

import com.google.common.collect.ImmutableList;
import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.ReturnType;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class QueryModelBuilder extends OperationModelBuilderBase<QueryModel> {

  private final String parentTypeName;

  QueryModelBuilder(
      FieldDefinition query,
      String parentTypeName,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(query, entities, responsePayloads, context);
    this.parentTypeName = parentTypeName;
  }

  QueryModel build() throws SkipException {

    Optional<Directive> cqlSelectDirective = DirectiveHelper.getDirective("cql_select", operation);
    Optional<Integer> limit =
        cqlSelectDirective.flatMap(d -> DirectiveHelper.getIntArgument(d, "limit", context));
    Optional<Integer> pageSize =
        cqlSelectDirective.flatMap(d -> DirectiveHelper.getIntArgument(d, "pageSize", context));

    ReturnType returnType = getReturnType("Query " + operationName);
    EntityModel entity =
        returnType
            .getEntity()
            .filter(e -> e.getTarget() == EntityModel.Target.TABLE)
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Query %s: return type must reference an entity that maps to a table",
                      operationName);
                  return SkipException.INSTANCE;
                });

    List<FieldModel> primaryKey = entity.getPrimaryKey();
    int pkIndex = 0;

    ImmutableList.Builder<String> pkArgumentNamesBuilder = ImmutableList.builder();
    Optional<String> pagingStateArgumentName = Optional.empty();

    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {
      if (isPagingState(inputValue)) {
        if (pagingStateArgumentName.isPresent()) {
          invalidMapping(
              "Query %s: @cql_pagingState can be used on at most one argument (found %s and %s)",
              operationName, pagingStateArgumentName.get(), inputValue.getName());
          foundErrors = true;
        }
        pagingStateArgumentName = Optional.of(inputValue.getName());
      } else {
        // Assume non-annotated fields are PK components in order:
        FieldModel pkField = primaryKey.get(pkIndex++);
        Type<?> inputType = inputValue.getType();
        if (!inputType.isEqualTo(pkField.getGraphqlType())) {
          invalidMapping(
              "Query %s: expected argument %s to have the same type as %s.%s",
              operationName,
              inputValue.getName(),
              entity.getGraphqlName(),
              pkField.getGraphqlName());
          foundErrors = true;
        }
        pkArgumentNamesBuilder.add(inputValue.getName());
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }

    ImmutableList<String> pkArgumentNames = pkArgumentNamesBuilder.build();
    List<FieldModel> partitionKey = entity.getPartitionKey();
    if (pkArgumentNames.size() < partitionKey.size()) {
      invalidMapping(
          "Query %s: expected to have at least enough arguments to cover the partition key "
              + "(%d needed, %d provided).",
          operationName, partitionKey.size(), pkArgumentNames.size());
      throw SkipException.INSTANCE;
    }

    return new QueryModel(
        parentTypeName,
        operation,
        entity,
        pkArgumentNames,
        pagingStateArgumentName,
        limit,
        pageSize,
        returnType);
  }

  private boolean isPagingState(InputValueDefinition inputValue) throws SkipException {
    boolean hasDirective = DirectiveHelper.getDirective("cql_pagingState", inputValue).isPresent();
    if (!hasDirective) {
      return false;
    }
    Type<?> type = TypeHelper.unwrapNonNull(inputValue.getType());
    if (!(type instanceof TypeName)
        || !((TypeName) type).getName().equals(Scalars.GraphQLString.getName())) {
      invalidMapping(
          "Query %s: argument %s annotated with @cql_pagingState must have type %s",
          operationName, inputValue.getName(), Scalars.GraphQLString.getName());
      throw SkipException.INSTANCE;
    }
    return true;
  }
}
