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
package io.stargate.db.datastore;

import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Regular;

import io.stargate.db.datastore.ValidatingDataStore.QueryAssert;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Sanity checks for {@link ValidatingDataStore}. */
public class ValidatingDataStoreTest extends AbstractDataStoreTest {

  protected static final Table table =
      ImmutableTable.builder()
          .keyspace("test_datastore")
          .name("table1")
          .addColumns(
              ImmutableColumn.builder().name("key").type(Type.Text).kind(PartitionKey).build())
          .addColumns(
              ImmutableColumn.builder().name("test_value").type(Type.Double).kind(Regular).build())
          .build();

  private static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("test_datastore").addTables(table).build();

  private static final Schema schema = ImmutableSchema.builder().addKeyspaces(keyspace).build();
  private QueryAssert queryAssert;
  private BuiltQuery<?> query;

  @Override
  protected Schema schema() {
    return schema;
  }

  @BeforeEach
  void setExpectations() {
    queryAssert =
        withQuery(table, "SELECT * FROM %s WHERE key > ? AND key < ?", "a", "x").returningNothing();

    query =
        datastore()
            .queryBuilder()
            .select()
            .star()
            .from(table)
            .where("key", Predicate.GT, "a")
            .where("key", Predicate.LT)
            .build();
  }

  @Test
  void testPrepareAndBind() throws Exception {
    Query<?> prepared = datastore().prepare(query).get();
    BoundQuery bound = prepared.bind("x");
    datastore().execute(bound);
    queryAssert.assertExecuteCount().isEqualTo(1);
  }

  @Test
  void testUnpreparedExecution() throws Exception {
    AbstractBound<?> bound = query.bind("x");
    datastore().execute(bound);
    queryAssert.assertExecuteCount().isEqualTo(1);
  }

  @Test
  void testPreparedNotExecuted() throws Exception {
    datastore().prepare(query).get();

    Assertions.assertThatThrownBy(this::checkExpectedExecutions)
        .hasMessageContaining("query was prepared but never executed");

    resetExpectations();
  }

  @Test
  void testMissedExpectation() throws Exception {
    Assertions.assertThatThrownBy(this::checkExpectedExecutions)
        .hasMessageContaining("No queries were executed");

    resetExpectations();
  }
}
