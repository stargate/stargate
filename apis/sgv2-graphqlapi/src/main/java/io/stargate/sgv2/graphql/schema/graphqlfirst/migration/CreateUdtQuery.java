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
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import java.util.List;
import java.util.stream.Collectors;

public class CreateUdtQuery extends MigrationQuery {

  private final Udt type;

  public CreateUdtQuery(String keyspaceName, Udt type) {
    super(keyspaceName);
    this.type = type;
  }

  public String getTypeName() {
    return type.getName();
  }

  @Override
  public Query build() {
    return new QueryBuilder()
        .create()
        .type(keyspaceName, type.getName())
        .column(buildFields())
        .build();
  }

  private List<Column> buildFields() {
    return type.getFieldsMap().entrySet().stream()
        .map(
            e ->
                ImmutableColumn.builder()
                    .name(e.getKey())
                    .type(TypeSpecs.format(e.getValue()))
                    .kind(Column.Kind.REGULAR)
                    .build())
        .collect(Collectors.toList());
  }

  @Override
  public String getDescription() {
    return "Create UDT " + type.getName();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must create a UDT before it gets referenced
    return that.addsReferenceTo(type.getName());
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return references(udtName, this.type);
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
