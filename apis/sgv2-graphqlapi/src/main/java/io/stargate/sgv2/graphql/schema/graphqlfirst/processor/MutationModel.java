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
import io.stargate.bridge.proto.QueryOuterClass.Consistency;
import java.util.Optional;

public abstract class MutationModel extends OperationModel {

  private final EntityModel entity;
  private final ReturnType returnType;
  private final Optional<Consistency> consistencyLevel;
  private final Optional<Consistency> serialConsistencyLevel;

  public MutationModel(
      String parentTypeName,
      FieldDefinition field,
      EntityModel entity,
      ReturnType returnType,
      Optional<Consistency> consistencyLevel,
      Optional<Consistency> serialConsistencyLevel) {
    super(parentTypeName, field);
    this.entity = entity;
    this.returnType = returnType;
    this.consistencyLevel = consistencyLevel;
    this.serialConsistencyLevel = serialConsistencyLevel;
  }

  public EntityModel getEntity() {
    return entity;
  }

  public ReturnType getReturnType() {
    return returnType;
  }

  public Optional<Consistency> getConsistencyLevel() {
    return consistencyLevel;
  }

  public Optional<Consistency> getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }
}
