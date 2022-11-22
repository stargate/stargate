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
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.schema.Coercing;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.QueryModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import io.stargate.graphql.schema.scalars.CqlScalar;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class QueryFetcher extends DeployedFetcher<Object> {

  @SuppressWarnings("unchecked")
  private static final Coercing<ByteBuffer, String> BLOB_COERCING =
      (Coercing<ByteBuffer, String>) CqlScalar.BLOB.getGraphqlType().getCoercing();

  private final QueryModel model;

  public QueryFetcher(QueryModel model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  protected Parameters buildParameters(DataFetchingEnvironment environment) {
    Optional<ByteBuffer> pagingState =
        model
            .getPagingStateArgumentName()
            .filter(environment::containsArgument)
            .map(name -> BLOB_COERCING.parseValue(environment.<String>getArgument(name)));
    int pageSize = model.getPageSize().orElse(DEFAULT_PAGE_SIZE);
    ConsistencyLevel consistencyLevel = model.getConsistencyLevel().orElse(DEFAULT_CONSISTENCY);

    if (!pagingState.isPresent()
        && pageSize == DEFAULT_PAGE_SIZE
        && consistencyLevel == DEFAULT_CONSISTENCY) {
      return DEFAULT_PARAMETERS;
    } else {
      return DEFAULT_PARAMETERS
          .toBuilder()
          .pagingState(pagingState)
          .pageSize(pageSize)
          .consistencyLevel(consistencyLevel)
          .build();
    }
  }

  @Override
  protected Object get(DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws UnauthorizedException {
    Keyspace keyspace =
        context.getDataStore().schema().keyspace(model.getEntity().getKeyspaceName());

    ReturnType returnType = model.getReturnType();

    List<BuiltCondition> whereConditions =
        bindWhere(
            model.getWhereConditions(),
            environment::containsArgument,
            environment::getArgument,
            model.getEntity()::validateNoFiltering,
            keyspace);

    ResultSet resultSet =
        query(
            model.getEntity(),
            whereConditions,
            model.getLimit(),
            buildParameters(environment),
            environment.getContext());
    Object entityData =
        returnType.isList()
            ? toEntities(resultSet, model.getEntity())
            : toSingleEntity(resultSet, model.getEntity());

    if (returnType instanceof ResponsePayloadModel) {
      ResponsePayloadModel payloadModel = (ResponsePayloadModel) returnType;
      assert payloadModel.getEntityField().isPresent(); // already checked while building the model
      String entityFieldName = payloadModel.getEntityField().get().getName();
      Map<String, Object> response = new HashMap<>();
      response.put(entityFieldName, entityData);
      if (payloadModel.getTechnicalFields().contains(TechnicalField.PAGING_STATE)) {
        ByteBuffer nextPagingState = resultSet.getPagingState();
        if (nextPagingState != null) {
          response.put(
              TechnicalField.PAGING_STATE.getGraphqlName(),
              BLOB_COERCING.serialize(nextPagingState));
        }
      }
      return response;
    } else {
      return entityData;
    }
  }
}
