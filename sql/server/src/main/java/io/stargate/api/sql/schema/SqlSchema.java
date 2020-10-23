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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class SqlSchema implements Schema {
  public abstract String name();

  public abstract Set<StorageTable> tables();

  @Override
  public Table getTable(String name) {
    return tables().stream()
        .filter(t -> t.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + name));
  }

  @Override
  public Set<String> getTableNames() {
    return tables().stream().map(StorageTable::name).collect(Collectors.toSet());
  }

  @Override
  public RelProtoDataType getType(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema getSubSchema(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    throw new UnsupportedOperationException();
  }
}
