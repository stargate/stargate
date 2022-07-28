/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class Table extends AbstractTable {
  private static final long serialVersionUID = 5913904335622827700L;

  public abstract List<Index> indexes();

  @Value.Lazy
  Map<String, Index> indexMap() {
    return indexes().stream().collect(Collectors.toMap(Index::name, Function.identity()));
  }

  public Index index(String name) {
    return indexMap().get(name);
  }

  public static Table create(
      String keyspace,
      String name,
      Iterable<Column> columns,
      Iterable<Index> indexes,
      String comment,
      int ttl) {
    return ImmutableTable.builder()
        .keyspace(keyspace)
        .name(name)
        .columns(columns)
        .indexes(indexes)
        .comment(comment)
        .ttl(ttl)
        .build();
  }

  public static Table create(
      String keyspace, String name, Iterable<Column> columns, Iterable<Index> indexes) {
    return ImmutableTable.builder()
        .keyspace(keyspace)
        .name(name)
        .columns(columns)
        .indexes(indexes)
        .build();
  }

  @Value.Default
  @Override
  public String comment() {
    return "";
  }

  @Value.Default
  @Override
  public int ttl() {
    return 0;
  }

  @Override
  public int priority() {
    return 0;
  }

  @Override
  public String toString() {
    StringBuilder tableBuilder = new StringBuilder();
    String name = name();
    tableBuilder.append("Table '").append(name).append("'");
    tableBuilder.append(":\n");
    tableBuilder.append(
        columns().stream()
            .map(
                c ->
                    "    "
                        + c.name()
                        + " "
                        + c.type()
                        + (c.kind() == Column.Kind.Regular ? "" : (" " + c.kind())))
            .collect(Collectors.joining("\n")));
    if (!indexes().isEmpty()) {
      tableBuilder.append("\n");
      tableBuilder.append("\n  Table '").append(name).append("' indexes:\n");
      tableBuilder.append(
          indexes().stream().map(idx -> "    " + idx.toString()).collect(Collectors.joining("\n")));
      tableBuilder.append("\n");
    }
    if (!comment().isEmpty()) {
      tableBuilder.append("\n  Table '").append(name).append("' comment: " + comment());
    }
    tableBuilder.append("\n  Table '").append(name).append("' ttl: " + ttl());
    tableBuilder.append("\n");
    return tableBuilder.toString();
  }

  @Override
  public String indexTypeName() {
    return "Table: " + name();
  }

  @Override
  public int schemaHashCode() {
    return SchemaHash.combine(
        name().hashCode(),
        keyspace().hashCode(),
        SchemaHash.hash(columns()),
        SchemaHash.hash(indexes()),
        comment().hashCode(),
        ttl());
  }
}
