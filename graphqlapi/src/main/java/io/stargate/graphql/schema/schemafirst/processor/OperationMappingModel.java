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
import graphql.schema.FieldCoordinates;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;

/** A GraphQL operation that will be translated into a CQL query. */
public abstract class OperationMappingModel {

  private final FieldCoordinates coordinates;

  protected OperationMappingModel(String parentTypeName, FieldDefinition field) {
    this.coordinates = FieldCoordinates.coordinates(parentTypeName, field.getName());
  }

  public FieldCoordinates getCoordinates() {
    return coordinates;
  }

  public abstract DataFetcher<?> getDataFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory);
}
