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

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Tuple;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.Schema;
import java.util.List;

/** A DDL query to be executed as part of a migration. */
public abstract class MigrationQuery {

  protected String keyspaceName;

  protected MigrationQuery(String keyspaceName) {
    this.keyspaceName = keyspaceName;
  }

  public abstract Query build();

  public abstract String getDescription();

  public abstract boolean mustRunBefore(MigrationQuery that);

  /** Whether the query will introduce a new column/field that uses the given UDT. */
  protected abstract boolean addsReferenceTo(String udtName);

  /** Whether the query will remove a column/field that was using the given UDT. */
  protected abstract boolean dropsReferenceTo(String udtName);

  protected boolean references(TypeSpec type, String udtName) {
    switch (type.getSpecCase()) {
      case UDT:
        Udt udt = type.getUdt();
        return (udt.getName().equals(udtName))
            || udt.getFieldsMap().values().stream().anyMatch(t -> references(t, udtName));
      case LIST:
        return references(type.getList().getElement(), udtName);
      case SET:
        return references(type.getSet().getElement(), udtName);
      case TUPLE:
        Tuple tuple = type.getTuple();
        return tuple.getElementsList().stream().anyMatch(t -> references(t, udtName));
      default:
        return false;
    }
  }

  protected boolean references(String udtName, Schema.CqlTable table) {
    return references(udtName, table.getPartitionKeyColumnsList())
        || references(udtName, table.getClusteringKeyColumnsList())
        || references(udtName, table.getColumnsList())
        || references(udtName, table.getStaticColumnsList());
  }

  private boolean references(String udtName, List<QueryOuterClass.ColumnSpec> columns) {
    return columns.stream().anyMatch(c -> references(c.getType(), udtName));
  }

  protected boolean references(String udtName, Udt type) {
    return type.getFieldsMap().values().stream().anyMatch(t -> references(t, udtName));
  }
}
