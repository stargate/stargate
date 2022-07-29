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
package io.stargate.db.schema;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** Represents a qualified table name. */
@Value.Immutable
public interface TableName extends QualifiedSchemaEntity {

  static TableName of(Collection<Column> columns) {
    Set<TableName> tables =
        columns.stream()
            .map(
                column ->
                    ImmutableTableName.builder()
                        .keyspace(Objects.requireNonNull(column.keyspace()))
                        .name(Objects.requireNonNull(column.table()))
                        .build())
            .collect(Collectors.toSet());

    if (tables.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing table information in the provided set of columns.");
    }

    if (tables.size() > 1) {
      throw new IllegalArgumentException("Too many tables are referenced: " + tables);
    }

    return tables.iterator().next();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default int schemaHashCode() {
    return SchemaHash.combine(SchemaHash.hashCode(keyspace()), SchemaHash.hashCode(name()));
  }
}
