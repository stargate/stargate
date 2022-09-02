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
package io.stargate.sgv2.graphql.schema.graphqlfirst.migration;

import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;

public class AddUdtFieldQuery extends MigrationQuery {

  private final String udtName;
  private final String fieldName;
  private final TypeSpec fieldType;

  public AddUdtFieldQuery(
      String keyspaceName, String udtName, String fieldName, TypeSpec fieldType) {
    super(keyspaceName);
    this.udtName = udtName;
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

  @Override
  public QueryOuterClass.Query build() {
    return new QueryBuilder()
        .alter()
        .type(keyspaceName, udtName)
        .addColumn(fieldName, TypeSpecs.format(fieldType))
        .build();
  }

  @Override
  public String getDescription() {
    return String.format("Add field %s to UDT %s", fieldName, udtName);
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // No other migration queries depend on the existence of a UDT field
    return false;
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return references(fieldType, udtName);
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
