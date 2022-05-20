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
import io.stargate.bridge.proto.QueryOuterClass.Consistency;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed.InsertFetcher;
import java.util.Optional;

public class InsertModel extends MutationModel {

  private final String entityArgumentName;
  private final Optional<ResponsePayloadModel> responsePayload;
  private final boolean ifNotExists;
  private final Optional<String> cqlTimestampArgumentName;
  private final Optional<Integer> ttl;
  private final boolean isList;
  private final CqlKeyspaceDescribe keyspace;

  InsertModel(
      String parentTypeName,
      FieldDefinition field,
      EntityModel entity,
      String entityArgumentName,
      Optional<ResponsePayloadModel> responsePayload,
      boolean ifNotExists,
      Optional<Consistency> consistencyLevel,
      Optional<Consistency> serialConsistencyLevel,
      Optional<Integer> ttl,
      ReturnType returnType,
      Optional<String> cqlTimestampArgumentName,
      boolean isList,
      CqlKeyspaceDescribe keyspace) {
    super(parentTypeName, field, entity, returnType, consistencyLevel, serialConsistencyLevel);
    this.entityArgumentName = entityArgumentName;
    this.responsePayload = responsePayload;
    this.ifNotExists = ifNotExists;
    this.ttl = ttl;
    this.cqlTimestampArgumentName = cqlTimestampArgumentName;
    this.isList = isList;
    this.keyspace = keyspace;
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

  public Optional<String> getCqlTimestampArgumentName() {
    return cqlTimestampArgumentName;
  }

  public Optional<Integer> getTtl() {
    return ttl;
  }

  public boolean isList() {
    return isList;
  }

  @Override
  public DataFetcher<?> getDataFetcher(MappingModel mappingModel) {
    return new InsertFetcher(this, mappingModel, keyspace);
  }
}
