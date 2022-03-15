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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import graphql.language.FieldDefinition;
import graphql.schema.DataFetcher;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed.UpdateFetcher;
import java.util.List;
import java.util.Optional;

public class UpdateModel extends MutationModel {

  private final List<ConditionModel> whereConditions;
  private final List<ConditionModel> ifConditions;
  private final Optional<String> entityArgumentName;
  private final Optional<ResponsePayloadModel> responsePayload;
  private final boolean ifExists;
  private final List<IncrementModel> incrementModels;
  private final Optional<Integer> ttl;
  private final Optional<String> cqlTimestampArgumentName;
  private final CqlKeyspaceDescribe keyspace;

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
      Optional<Consistency> consistencyLevel,
      Optional<Consistency> serialConsistencyLevel,
      Optional<Integer> ttl,
      Optional<String> cqlTimestampArgumentName,
      CqlKeyspaceDescribe keyspace) {
    super(parentTypeName, field, entity, returnType, consistencyLevel, serialConsistencyLevel);
    this.whereConditions = whereConditions;
    this.ifConditions = ifConditions;
    this.entityArgumentName = entityArgumentName;
    this.responsePayload = responsePayload;
    this.ifExists = ifExists;
    this.incrementModels = incrementModels;
    this.ttl = ttl;
    this.cqlTimestampArgumentName = cqlTimestampArgumentName;
    this.keyspace = keyspace;
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

  public Optional<ResponsePayloadModel> getResponsePayload() {
    return responsePayload;
  }

  public List<IncrementModel> getIncrementModels() {
    return incrementModels;
  }

  public Optional<Integer> getTtl() {
    return ttl;
  }

  public Optional<String> getCqlTimestampArgumentName() {
    return cqlTimestampArgumentName;
  }

  @Override
  public DataFetcher<?> getDataFetcher(MappingModel mappingModel) {
    return new UpdateFetcher(this, mappingModel, keyspace);
  }
}
