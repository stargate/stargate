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
package io.stargate.sgv2.api.common.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.sgv2.api.common.cql.Expression.And;
import io.stargate.sgv2.api.common.cql.Expression.Expression;
import io.stargate.sgv2.api.common.cql.Expression.Variable;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryBuilderTest {

  @ParameterizedTest
  @MethodSource("sampleQueries")
  @DisplayName("Should generate expected CQL string")
  public void generateCql(String actualCql, String expectedCql) {
    assertThat(actualCql).isEqualTo(expectedCql);
  }

  @SuppressWarnings("PMD.ExcessiveMethodLength")
  public static Arguments[] sampleQueries() {
    return new Arguments[] {
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .build()
              .getCql(),
          "CREATE KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("Ks")
              .withReplication(Replication.simpleStrategy(1))
              .build()
              .getCql(),
          "CREATE KEYSPACE \"Ks\" "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("ks")
              .ifNotExists()
              .withReplication(Replication.simpleStrategy(1))
              .build()
              .getCql(),
          "CREATE KEYSPACE IF NOT EXISTS ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("ks")
              .ifNotExists()
              .withReplication(
                  Replication.networkTopologyStrategy(
                      new LinkedHashMap<String, Integer>() {
                        {
                          put("dc1", 3);
                          put("dc2", 4);
                        }
                      }))
              .build()
              .getCql(),
          "CREATE KEYSPACE IF NOT EXISTS ks "
              + "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 4 }"),
      arguments(
          new QueryBuilder()
              .alter()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .build()
              .getCql(),
          "ALTER KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .alter()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .andDurableWrites(false)
              .build()
              .getCql(),
          "ALTER KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 } "
              + "AND durable_writes = false"),
      arguments(new QueryBuilder().drop().keyspace("ks").build().getCql(), "DROP KEYSPACE ks"),
      arguments(
          new QueryBuilder().drop().keyspace("ks").ifExists().build().getCql(),
          "DROP KEYSPACE IF EXISTS ks"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build()
              .getCql(),
          "CREATE TABLE ks.tbl (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("Ks", "Tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build()
              .getCql(),
          "CREATE TABLE \"Ks\".\"Tbl\" (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build()
              .getCql(),
          "CREATE TABLE tbl (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .ifNotExists()
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build()
              .getCql(),
          "CREATE TABLE IF NOT EXISTS ks.tbl (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .ifNotExists()
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .column("cc", "text", Column.Kind.CLUSTERING, Column.Order.DESC)
              .column("v", "int")
              .column("s", "int", Column.Kind.STATIC)
              .build()
              .getCql(),
          "CREATE TABLE IF NOT EXISTS ks.tbl "
              + "(k int, cc text, v int, s int STATIC, PRIMARY KEY ((k), cc)) "
              + "WITH CLUSTERING ORDER BY (cc DESC)"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .withComment("'test' comment")
              .withDefaultTTL(3600)
              .build()
              .getCql(),
          "CREATE TABLE ks.tbl (k int, PRIMARY KEY ((k))) "
              + "WITH comment = '''test'' comment' "
              + "AND default_time_to_live = 3600"),
      arguments(
          new QueryBuilder()
              .alter()
              .table("ks", "tbl")
              .addColumn("c", "int")
              .addColumn("d", "int")
              .build()
              .getCql(),
          "ALTER TABLE ks.tbl ADD (c int, d int)"),
      arguments(
          new QueryBuilder()
              .alter()
              .table("ks", "tbl")
              .dropColumn("c")
              .dropColumn("d")
              .build()
              .getCql(),
          "ALTER TABLE ks.tbl DROP (c, d)"),
      arguments(
          new QueryBuilder()
              .alter()
              .table("ks", "tbl")
              .renameColumn("c", "c2")
              .renameColumn("d", "d2")
              .build()
              .getCql(),
          "ALTER TABLE ks.tbl RENAME c TO c2 AND d TO d2"),
      arguments(new QueryBuilder().drop().table("ks", "tbl").build().getCql(), "DROP TABLE ks.tbl"),
      arguments(
          new QueryBuilder().drop().table("ks", "tbl").ifExists().build().getCql(),
          "DROP TABLE IF EXISTS ks.tbl"),
      arguments(
          new QueryBuilder().truncate().table("ks", "tbl").build().getCql(), "TRUNCATE ks.tbl"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").build().getCql(),
          "CREATE INDEX ON ks.tbl (c)"),
      arguments(
          new QueryBuilder().create().index("idx").on("ks", "tbl").column("c").build().getCql(),
          "CREATE INDEX idx ON ks.tbl (c)"),
      arguments(
          new QueryBuilder()
              .create()
              .index("idx")
              .ifNotExists()
              .on("ks", "tbl")
              .column("c")
              .build()
              .getCql(),
          "CREATE INDEX IF NOT EXISTS idx ON ks.tbl (c)"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .indexEntries()
              .build()
              .getCql(),
          "CREATE INDEX ON ks.tbl (ENTRIES(c))"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .indexFull()
              .build()
              .getCql(),
          "CREATE INDEX ON ks.tbl (FULL(c))"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .indexKeys()
              .build()
              .getCql(),
          "CREATE INDEX ON ks.tbl (KEYS(c))"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .indexValues()
              .build()
              .getCql(),
          "CREATE INDEX ON ks.tbl (VALUES(c))"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .custom("IndexClass")
              .build()
              .getCql(),
          "CREATE CUSTOM INDEX ON ks.tbl (c) USING 'IndexClass'"),
      arguments(
          new QueryBuilder().drop().index("idx").ifExists().build().getCql(),
          "DROP INDEX IF EXISTS idx"),
      arguments(new QueryBuilder().drop().index("ks", "idx").build().getCql(), "DROP INDEX ks.idx"),
      arguments(
          new QueryBuilder()
              .create()
              .materializedView("ks", "v")
              .asSelect()
              .column("a", Column.Kind.PARTITION_KEY)
              .column("b")
              .column("c")
              .from("ks", "tbl")
              .build()
              .getCql(),
          "CREATE MATERIALIZED VIEW ks.v "
              + "AS SELECT a, b, c "
              + "FROM ks.tbl "
              + "WHERE a IS NOT NULL "
              + "AND b IS NOT NULL "
              + "AND c IS NOT NULL "
              + "PRIMARY KEY ((a))"),
      arguments(
          new QueryBuilder().drop().materializedView("ks", "tbl").build().getCql(),
          "DROP MATERIALIZED VIEW ks.tbl"),
      arguments(
          new QueryBuilder().drop().materializedView("ks", "tbl").ifExists().build().getCql(),
          "DROP MATERIALIZED VIEW IF EXISTS ks.tbl"),
      arguments(
          new QueryBuilder()
              .create()
              .type("ks", "t")
              .column("a", "int")
              .column("b", "int")
              .build()
              .getCql(),
          "CREATE TYPE ks.t (a int, b int)"),
      arguments(
          new QueryBuilder()
              .alter()
              .type("ks", "t")
              .renameColumn("a", "a2")
              .renameColumn("b", "b2")
              .build()
              .getCql(),
          "ALTER TYPE ks.t RENAME a TO a2 AND b TO b2"),
      arguments(new QueryBuilder().drop().type("ks", "t").build().getCql(), "DROP TYPE ks.t"),
      arguments(
          new QueryBuilder().drop().type("ks", "t").ifExists().build().getCql(),
          "DROP TYPE IF EXISTS ks.t"),
      arguments(
          new QueryBuilder()
              .alter()
              .type("ks", "t")
              .addColumn("c", "int")
              .addColumn("d", "int")
              .build()
              .getCql(),
          "ALTER TYPE ks.t ADD c int, d int"),
      arguments(
          new QueryBuilder().select().from("ks", "tbl").build().getCql(), "SELECT * FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().column("a", "b", "c").from("ks", "tbl").build().getCql(),
          "SELECT a, b, c FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").build().getCql(),
          "SELECT COUNT(a) FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count().from("ks", "tbl").build().getCql(),
          "SELECT COUNT(1) FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit(1).build().getCql(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .count("a")
              .from("ks", "tbl")
              .limit(Values.of(1))
              .build()
              .getCql(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT ?"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit().build().getCql(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT ?"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .from("ks", "tbl")
              .limit(1)
              .vsearch("vector_column")
              .build()
              .getCql(),
          "SELECT a, b, c FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityCosine("vector_column", Values.of(List.of(Values.of(1.0f))))
              .from("ks", "tbl")
              .limit(1)
              .vsearch("vector_column")
              .build()
              .getCql(),
          "SELECT a, b, c, SIMILARITY_COSINE(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityDotProduct("vector_column", Values.of(List.of(Values.of(1.0f))))
              .from("ks", "tbl")
              .limit(1)
              .vsearch("vector_column")
              .build()
              .getCql(),
          "SELECT a, b, c, SIMILARITY_DOT_PRODUCT(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityEuclidean("vector_column", Values.of(List.of(Values.of(1.0f))))
              .from("ks", "tbl")
              .limit(1)
              .vsearch("vector_column")
              .build()
              .getCql(),
          "SELECT a, b, c, SIMILARITY_EUCLIDEAN(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1"),
    };
  }

  @Test
  public void generateBatchQuery() {
    Expression<BuiltCondition> expression =
        And.of(Variable.of(BuiltCondition.of("id", Predicate.EQ, Values.of(1))));
    BatchQuery batchQuery =
        new QueryBuilder().select().from("ks", "tbl").where(expression).buildForBatch();
    assertThat(batchQuery.getCql()).isEqualTo("SELECT * FROM ks.tbl WHERE id = ?");
    assertThat(batchQuery.getValues().getValuesList()).containsOnly(Values.of(1));
  }

  @Test
  public void failWhenBatchQueryHasParameters() {
    assertThatThrownBy(
            () ->
                new QueryBuilder()
                    .select()
                    .from("ks", "tbl")
                    .parameters(QueryOuterClass.QueryParameters.newBuilder().build())
                    .buildForBatch())
        .isInstanceOf(IllegalStateException.class);
  }
}
