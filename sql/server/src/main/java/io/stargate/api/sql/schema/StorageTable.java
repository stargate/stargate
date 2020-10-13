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
package io.stargate.api.sql.schema;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class StorageTable extends AbstractTable {
  public abstract Keyspace keyspace();

  public abstract Table table();

  public final String name() {
    return table().name();
  }

  public final List<Column> columns() {
    return table().columns();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

    columns()
        .forEach(
            c ->
                fieldInfo
                    .add(c.name(), TypeUtils.toCalciteType(c.type()))
                    .nullable(Objects.requireNonNull(c.kind()).isPrimaryKeyKind()));

    return fieldInfo.build();
  }

  public static StorageTable from(Keyspace ks, Table t) {
    return ImmutableStorageTable.builder().keyspace(ks).table(t).build();
  }
}
