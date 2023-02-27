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

import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import io.stargate.graphql.schema.graphqlfirst.processor.ArgumentDirectiveModelsBuilder.OperationType;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

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

    Optional<Directive> cqlSelectDirective =
        DirectiveHelper.getDirective(CqlDirectives.SELECT, operation);
    Optional<Integer> limit =
        cqlSelectDirective.flatMap(
            d -> DirectiveHelper.getIntArgument(d, CqlDirectives.SELECT_LIMIT, context));
    Optional<Integer> pageSize =
        cqlSelectDirective.flatMap(
            d -> DirectiveHelper.getIntArgument(d, CqlDirectives.SELECT_PAGE_SIZE, context));
    Optional<ConsistencyLevel> consistencyLevel =
        cqlSelectDirective.flatMap(
            d ->
                DirectiveHelper.getEnumArgument(
                    d, CqlDirectives.SELECT_CONSISTENCY_LEVEL, ConsistencyLevel.class, context));

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

    Optional<String> pagingStateArgumentName =
        findFieldNameWithDirective(CqlDirectives.PAGING_STATE, Scalars.GraphQLString);
    ArgumentDirectiveModels conditions =
        new ArgumentDirectiveModelsBuilder(
                operation, OperationType.SELECT, entity, entities, context)
            .build();
    List<ConditionModel> whereConditions = conditions.getWhereConditions();
    validateNoFiltering(whereConditions, entity);

    return new QueryModel(
        parentTypeName,
        operation,
        entity,
        whereConditions,
        pagingStateArgumentName,
        limit,
        pageSize,
        consistencyLevel,
        returnType);
  }
}
