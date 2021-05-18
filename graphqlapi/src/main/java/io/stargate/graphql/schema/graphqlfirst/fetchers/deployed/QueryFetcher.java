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
import io.stargate.db.datastore.DataStore;
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

public class QueryFetcher extends DeployedFetcher<Object> {

  @SuppressWarnings("unchecked")
  private static final Coercing<ByteBuffer, String> BLOB_COERCING =
      CqlScalar.BLOB.getGraphqlType().getCoercing();

  private final QueryModel model;

  public QueryFetcher(QueryModel model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  @Override
  protected Object get(
      DataFetchingEnvironment environment, DataStore dataStore, StargateGraphqlContext context)
      throws UnauthorizedException {
    Keyspace keyspace = dataStore.schema().keyspace(model.getEntity().getKeyspaceName());

    Optional<ByteBuffer> pagingState =
        model
            .getPagingStateArgumentName()
            .filter(environment::containsArgument)
            .map(name -> BLOB_COERCING.parseValue(environment.<String>getArgument(name)));

    ReturnType returnType = model.getReturnType();

    List<BuiltCondition> whereConditions =
        bind(
            model.getWhereConditions(),
            model.getEntity(),
            environment::containsArgument,
            environment::getArgument,
            keyspace);

    ResultSet resultSet =
        query(
            model.getEntity(),
            whereConditions,
            pagingState,
            model.getLimit(),
            model.getPageSize(),
            dataStore,
            environment.getContext());
    Object entityData =
        returnType.isEntityList()
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
