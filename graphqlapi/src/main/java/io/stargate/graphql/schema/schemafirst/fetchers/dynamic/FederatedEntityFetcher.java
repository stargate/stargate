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

import com.apollographql.federation.graphqljava._Entity;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.MappingModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Executes the {@code _entities} query of GraphQL federation.
 *
 * @see <a
 *     href="https://www.apollographql.com/docs/federation/federation-spec/#resolve-requests-for-entities">The
 *     Apollo Federation spec</a>
 */
public class FederatedEntityFetcher extends DynamicFetcher<List<FederatedEntity>> {

  private final MappingModel mappingModel;

  public FederatedEntityFetcher(
      MappingModel mappingModel,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
    this.mappingModel = mappingModel;
  }

  @Override
  protected List<FederatedEntity> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

    List<FederatedEntity> result = new ArrayList<>();
    for (Map<String, Object> representation :
        environment.<List<Map<String, Object>>>getArgument(_Entity.argumentName)) {
      result.add(getEntity(representation, dataStore, authenticationSubject));
    }
    return result;
  }

  private FederatedEntity getEntity(
      Map<String, Object> representation,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {
    Object rawTypeName = representation.get("__typename");
    if (!(rawTypeName instanceof String)) {
      throw new IllegalArgumentException(
          "Entity representations must contain a '__typename' string field");
    }
    String entityName = (String) rawTypeName;
    EntityMappingModel entity = mappingModel.getEntities().get(entityName);
    if (entity == null) {
      throw new IllegalArgumentException(String.format("Unknown entity type %s", entityName));
    }
    return FederatedEntity.wrap(
        entity,
        querySingleEntity(
            entity, representation, Optional.empty(), dataStore, authenticationSubject));
  }
}
