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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.javatuples.Pair;

@Value.Immutable(prehash = true)
public abstract class Keyspace implements SchemaEntity {

  private static final long serialVersionUID = -337891773492616286L;

  public abstract Set<Table> tables();

  public abstract List<UserDefinedType> userDefinedTypes();

  public abstract Map<String, String> replication();

  public abstract Optional<Boolean> durableWrites();

  @Value.Lazy
  Map<String, Table> tableMap() {
    return tables().stream().collect(Collectors.toMap(Table::name, Function.identity()));
  }

  public Table table(String name) {
    return tableMap().get(name);
  }

  @Value.Lazy
  Map<String, UserDefinedType> userDefinedTypeMap() {
    return userDefinedTypes().stream()
        .collect(Collectors.toMap(UserDefinedType::name, Function.identity()));
  }

  public UserDefinedType userDefinedType(String typeName) {
    return userDefinedTypeMap().get(typeName);
  }

  @Value.Lazy
  Map<String, MaterializedView> materializedViewMap() {
    return tables().stream()
        .flatMap(t -> t.indexes().stream())
        .filter(i -> i instanceof MaterializedView)
        .collect(Collectors.toMap(Index::name, i -> (MaterializedView) i));
  }

  public MaterializedView materializedView(String name) {
    return materializedViewMap().get(name);
  }

  @Value.Lazy
  Map<String, SecondaryIndex> secondaryIndexMap() {
    return tables().stream()
        .flatMap(t -> t.indexes().stream())
        .filter(i -> i instanceof SecondaryIndex)
        .collect(Collectors.toMap(Index::name, i -> (SecondaryIndex) i));
  }

  public SecondaryIndex secondaryIndex(String name) {
    return secondaryIndexMap().get(name);
  }

  @Value.Lazy
  Map<Index, Table> reverseIndexMap() {
    return tables().stream()
        .flatMap(t -> t.indexes().stream().map(i -> new Pair<>(t, i)))
        .collect(Collectors.toMap(Pair::getValue1, Pair::getValue0));
  }

  public Column getColumnFromTableOrIndex(String cf, String name) {
    // This is probably only a bit slower than constructing the column from the C* column
    Table table = table(cf);
    if (table != null) {
      Column c = table.column(name);

      if (c == null) {
        throw new IllegalArgumentException(
            String.format("%s does not contain the requested column %s", table, name));
      }

      return c;
    }

    MaterializedView mv = materializedView(cf);
    if (mv != null) {
      Column c = mv.column(name);

      if (c == null) {
        throw new IllegalArgumentException(
            String.format(
                "materialized view %s does not contain the requested column %s", mv, name));
      }

      return c;
    }

    SecondaryIndex si = secondaryIndex(cf);
    if (si != null) {
      if (si.column().name().equals(name)) {
        return si.column();
      } else {
        throw new RuntimeException("Secondary index does not contain the requested column");
      }
    }

    throw new RuntimeException("No table, MV, or secondary index matched the requested column");
  }

  public static Keyspace create(
      String name,
      Iterable<Table> tables,
      Iterable<UserDefinedType> userDefinedTypes,
      Map<String, String> replication,
      Optional<Boolean> durableWrites) {
    return ImmutableKeyspace.builder()
        .name(name)
        .addAllTables(tables)
        .addAllUserDefinedTypes(userDefinedTypes)
        .putAllReplication(replication)
        .durableWrites(durableWrites)
        .build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Keyspace '").append(name()).append("'\n");
    tables().forEach(table -> builder.append("  ").append(table.toString()));
    return builder.toString();
  }

  public AbstractTable tableOrMaterializedView(String name) {
    AbstractTable table = table(name);
    if (table == null) {
      return materializedView(name);
    }
    return table;
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  public int schemaHashCode() {
    return SchemaHashable.combine(
        SchemaHashable.hashCode(name()),
        SchemaHashable.hash(tables()),
        SchemaHashable.hash(userDefinedTypes()),
        SchemaHashable.hashCode(replication()),
        SchemaHashable.hashCode(durableWrites()));
  }
}
