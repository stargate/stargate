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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class MaterializedView extends AbstractTable implements Index {
  private static final long serialVersionUID = -2999120284516448661L;

  public static MaterializedView create(String keyspace, String name, Iterable<Column> columns) {
    return create(keyspace, name, columns, "", 0);
  }

  public static MaterializedView create(
      String keyspace, String name, Iterable<Column> columns, String comment, int ttl) {
    columns.forEach(
        c -> {
          Preconditions.checkState(
              c.kind() != null, "Column reference may not be used %s", c.name());
        });
    return ImmutableMaterializedView.builder()
        .keyspace(keyspace)
        .name(name)
        .addAllColumns(columns)
        .comment(comment)
        .ttl(ttl)
        .build();
  }

  public static MaterializedView reference(String keyspace, String name) {
    return ImmutableMaterializedView.builder().keyspace(keyspace).name(name).addColumns().build();
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
    return 1;
  }

  @Override
  public String indexTypeName() {
    return "Materialized view";
  }

  @Override
  public int schemaHashCode() {
    return SchemaHash.combine(
        SchemaHash.hashCode(name()),
        SchemaHash.hashCode(keyspace()),
        SchemaHash.hash(columns()),
        SchemaHash.hashCode(comment()),
        SchemaHash.hashCode(ttl()));
  }
}
