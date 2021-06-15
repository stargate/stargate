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
import io.stargate.graphql.schema.graphqlfirst.fetchers.deployed.UpdateFetcher;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class UpdateModel extends MutationModel {

  private final EntityModel entity;
  private final List<ConditionModel> whereConditions;
  private final List<ConditionModel> ifConditions;
  private final Optional<String> entityArgumentName;
  private final ReturnType returnType;
  private final Optional<ResponsePayloadModel> responsePayload;
  private final boolean ifExists;
  private final List<IncrementModel> incrementModels;

  public UpdateModel(
      String parentTypeName,
      FieldDefinition field,
      EntityModel entity,
      List<ConditionModel> whereConditions,
      List<ConditionModel> ifConditions,
      Optional<String> entityArgumentName,
      ReturnType returnType,
      Optional<ResponsePayloadModel> responsePayload,
      boolean ifExists,
      List<IncrementModel> incrementModels,
      Optional<ConsistencyLevel> consistencyLevel,
      Optional<ConsistencyLevel> serialConsistencyLevel) {
    super(parentTypeName, field, consistencyLevel, serialConsistencyLevel);
    this.entity = entity;
    this.whereConditions = whereConditions;
    this.ifConditions = ifConditions;
    this.entityArgumentName = entityArgumentName;
    this.returnType = returnType;
    this.responsePayload = responsePayload;
    this.ifExists = ifExists;
    this.incrementModels = incrementModels;
  }

  public EntityModel getEntity() {
    return entity;
  }

  public List<ConditionModel> getWhereConditions() {
    return whereConditions;
  }

  public List<ConditionModel> getIfConditions() {
    return ifConditions;
  }

  public boolean ifExists() {
    return ifExists;
  }

  public Optional<String> getEntityArgumentName() {
    return entityArgumentName;
  }

  public ReturnType getReturnType() {
    return returnType;
  }

  public Optional<ResponsePayloadModel> getResponsePayload() {
    return responsePayload;
  }

  public List<IncrementModel> getIncrementModels() {
    return incrementModels;
  }

  @Override
  public DataFetcher<?> getDataFetcher(MappingModel mappingModel) {
    return new UpdateFetcher(this, mappingModel);
  }
}
