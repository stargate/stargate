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
package io.stargate.graphql.schema.schemafirst.fetchers.dynamic;

import graphql.schema.Coercing;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.scalars.CqlScalar;
import io.stargate.graphql.schema.schemafirst.processor.MappingModel;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.ReturnType;
import io.stargate.graphql.schema.schemafirst.processor.QueryModel;
import io.stargate.graphql.schema.schemafirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.schemafirst.processor.ResponsePayloadModel.TechnicalField;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class QueryFetcher extends DynamicFetcher<Object> {

  @SuppressWarnings("unchecked")
  private static final Coercing<ByteBuffer, String> BLOB_COERCING =
      CqlScalar.BLOB.getGraphqlType().getCoercing();

  private final QueryModel model;

  public QueryFetcher(
      QueryModel model,
      MappingModel mappingModel,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(mappingModel, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Object get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {
    Keyspace keyspace = dataStore.schema().keyspace(model.getEntity().getKeyspaceName());

    Optional<ByteBuffer> pagingState =
        model
            .getPagingStateArgumentName()
            .filter(environment::containsArgument)
            .map(name -> BLOB_COERCING.parseValue(environment.<String>getArgument(name)));

    ReturnType returnType = model.getReturnType();

    ResultSet resultSet;
    Object entityData;
    if (returnType.isEntityList()) {
      resultSet =
          queryListOfEntities(
              model.getEntity(),
              environment.getArguments(),
              model.getPkArgumentNames(),
              pagingState,
              model.getLimit(),
              model.getPageSize(),
              dataStore,
              keyspace,
              authenticationSubject);
      entityData = toEntities(resultSet, model.getEntity());
    } else {
      resultSet =
          querySingleEntity(
              model.getEntity(),
              environment.getArguments(),
              model.getPkArgumentNames(),
              dataStore,
              keyspace,
              authenticationSubject);
      entityData = toSingleEntity(resultSet, model.getEntity());
    }

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
