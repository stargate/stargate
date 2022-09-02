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

import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;

public class DropUdtQuery extends MigrationQuery {

  private final Udt type;

  public DropUdtQuery(String keyspaceName, Udt type) {
    super(keyspaceName);
    this.type = type;
  }

  public String getTypeName() {
    return type.getName();
  }

  @Override
  public Query build() {
    return new QueryBuilder().drop().type(keyspaceName, type.getName()).ifExists().build();
  }

  @Override
  public String getDescription() {
    return "Drop UDT " + type.getName();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must keep "drop and recreate" operations in the correct order
    if (that instanceof CreateUdtQuery) {
      return type.getName().equals(((CreateUdtQuery) that).getTypeName());
    }
    // Must drop all references to a UDT before it gets dropped
    if (that instanceof DropUdtQuery) {
      return this.dropsReferenceTo(((DropUdtQuery) that).getTypeName());
    }
    return false;
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return false;
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return references(udtName, type);
  }
}
