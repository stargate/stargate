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

import graphql.language.FieldDefinition;
import graphql.schema.DataFetcher;
import io.stargate.graphql.schema.graphqlfirst.fetchers.deployed.InsertFetcher;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class InsertModel extends MutationModel {

  private final EntityModel entity;
  private final String entityArgumentName;
  private final Optional<ResponsePayloadModel> responsePayload;
  private final boolean ifNotExists;
  private final ReturnType returnType;
  private final boolean isList;

  InsertModel(
      String parentTypeName,
      FieldDefinition field,
      EntityModel entity,
      String entityArgumentName,
      Optional<ResponsePayloadModel> responsePayload,
      boolean ifNotExists,
      Optional<ConsistencyLevel> consistencyLevel,
      Optional<ConsistencyLevel> serialConsistencyLevel,
      ReturnType returnType,
      boolean isList) {
    super(parentTypeName, field, consistencyLevel, serialConsistencyLevel);
    this.entity = entity;
    this.entityArgumentName = entityArgumentName;
    this.responsePayload = responsePayload;
    this.ifNotExists = ifNotExists;
    this.returnType = returnType;
    this.isList = isList;
  }

  public EntityModel getEntity() {
    return entity;
  }

  public String getEntityArgumentName() {
    return entityArgumentName;
  }

  public Optional<ResponsePayloadModel> getResponsePayload() {
    return responsePayload;
  }

  public boolean ifNotExists() {
    return ifNotExists;
  }

  public ReturnType getReturnType() {
    return returnType;
  }

  public boolean isList() {
    return isList;
  }

  @Override
  public DataFetcher<?> getDataFetcher(MappingModel mappingModel) {
    return new InsertFetcher(this, mappingModel);
  }
}
