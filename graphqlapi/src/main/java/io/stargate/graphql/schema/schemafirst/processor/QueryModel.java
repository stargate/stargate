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

import graphql.language.FieldDefinition;
import graphql.schema.DataFetcher;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.QueryFetcher;
import java.util.List;

public class QueryModel extends OperationModel {

  // TODO implement more flexible rules
  // This is a basic implementation that only allows a single-entity SELECT by full primary key.
  // There will probably be significant changes when we support more scenarios: partial primary key
  // (returning multiple entities), index lookups, etc

  private final EntityModel entity;
  private final List<String> inputNames;
  private final boolean returnsList;

  QueryModel(
      String parentTypeName,
      FieldDefinition field,
      EntityModel entity,
      List<String> inputNames,
      boolean returnsList) {
    super(parentTypeName, field);
    this.entity = entity;
    this.inputNames = inputNames;
    this.returnsList = returnsList;
  }

  public EntityModel getEntity() {
    return entity;
  }

  public List<String> getInputNames() {
    return inputNames;
  }

  public boolean returnsList() {
    return returnsList;
  }

  @Override
  public DataFetcher<?> getDataFetcher(
      MappingModel mappingModel,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    return new QueryFetcher(
        this, mappingModel, authenticationService, authorizationService, dataStoreFactory);
  }
}
