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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.DeleteModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DeleteFetcher extends MutationFetcher<DeleteModel, DataFetcherResult<Object>> {

  public DeleteFetcher(DeleteModel model, MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    super(model, mappingModel, keyspace);
  }

  @Override
  protected MutationPayload<DataFetcherResult<Object>> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    EntityModel entityModel = model.getEntity();

    // We're either getting the values from a single entity argument, or individual PK field
    // arguments:
    java.util.function.Predicate<String> hasArgument;
    Function<String, Object> getArgument;
    if (model.getEntityArgumentName().isPresent()) {
      Map<String, Object> entity = environment.getArgument(model.getEntityArgumentName().get());
      hasArgument = entity::containsKey;
      getArgument = entity::get;
    } else {
      hasArgument = environment::containsArgument;
      getArgument = environment::getArgument;
    }

    List<BuiltCondition> whereConditions =
        bindWhere(
            model.getWhereConditions(),
            hasArgument,
            getArgument,
            entityModel::validateNoFiltering,
            keyspace);
    List<BuiltCondition> ifConditions =
        bindIf(model.getIfConditions(), hasArgument, getArgument, keyspace);
    Query query =
        new QueryBuilder()
            .delete()
            .from(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .where(whereConditions)
            .ifs(ifConditions)
            .ifExists(model.ifExists())
            .parameters(buildParameters())
            .build();

    List<TypedKeyValue> primaryKey = computePrimaryKey(model.getEntity(), whereConditions);

    return new MutationPayload<>(query, primaryKey, getDeleteOrUpdateResultBuilder(environment));
  }
}
