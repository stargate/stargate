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
package io.stargate.sgv2.common.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.api.DisplayName;
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

  public static Arguments[] sampleQueries() {
    return new Arguments[] {
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .build(),
          "CREATE KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("Ks")
              .withReplication(Replication.simpleStrategy(1))
              .build(),
          "CREATE KEYSPACE \"Ks\" "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .create()
              .keyspace("ks")
              .ifNotExists()
              .withReplication(Replication.simpleStrategy(1))
              .build(),
          "CREATE KEYSPACE IF NOT EXISTS ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .alter()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .build(),
          "ALTER KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"),
      arguments(
          new QueryBuilder()
              .alter()
              .keyspace("ks")
              .withReplication(Replication.simpleStrategy(1))
              .andDurableWrites(false)
              .build(),
          "ALTER KEYSPACE ks "
              + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 } "
              + "AND durable_writes = false"),
      arguments(new QueryBuilder().drop().keyspace("ks").build(), "DROP KEYSPACE ks"),
      arguments(
          new QueryBuilder().drop().keyspace("ks").ifExists().build(),
          "DROP KEYSPACE IF EXISTS ks"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build(),
          "CREATE TABLE ks.tbl (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("Ks", "Tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build(),
          "CREATE TABLE \"Ks\".\"Tbl\" (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("tbl")
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build(),
          "CREATE TABLE tbl (k int, PRIMARY KEY ((k)))"),
      arguments(
          new QueryBuilder()
              .create()
              .table("ks", "tbl")
              .ifNotExists()
              .column("k", "int", Column.Kind.PARTITION_KEY)
              .build(),
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
              .build(),
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
              .build(),
          "CREATE TABLE ks.tbl (k int, PRIMARY KEY ((k))) "
              + "WITH comment = '''test'' comment' "
              + "AND default_time_to_live = 3600"),
      arguments(
          new QueryBuilder()
              .alter()
              .table("ks", "tbl")
              .addColumn("c", "int")
              .addColumn("d", "int")
              .build(),
          "ALTER TABLE ks.tbl ADD (c int, d int)"),
      arguments(
          new QueryBuilder().alter().table("ks", "tbl").dropColumn("c").dropColumn("d").build(),
          "ALTER TABLE ks.tbl DROP (c, d)"),
      arguments(
          new QueryBuilder()
              .alter()
              .table("ks", "tbl")
              .renameColumn("c", "c2")
              .renameColumn("d", "d2")
              .build(),
          "ALTER TABLE ks.tbl RENAME c TO c2 AND d TO d2"),
      arguments(new QueryBuilder().drop().table("ks", "tbl").build(), "DROP TABLE ks.tbl"),
      arguments(
          new QueryBuilder().drop().table("ks", "tbl").ifExists().build(),
          "DROP TABLE IF EXISTS ks.tbl"),
      arguments(new QueryBuilder().truncate().table("ks", "tbl").build(), "TRUNCATE ks.tbl"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").build(),
          "CREATE INDEX ON ks.tbl (c)"),
      arguments(
          new QueryBuilder().create().index("idx").on("ks", "tbl").column("c").build(),
          "CREATE INDEX idx ON ks.tbl (c)"),
      arguments(
          new QueryBuilder()
              .create()
              .index("idx")
              .ifNotExists()
              .on("ks", "tbl")
              .column("c")
              .build(),
          "CREATE INDEX IF NOT EXISTS idx ON ks.tbl (c)"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").indexEntries().build(),
          "CREATE INDEX ON ks.tbl (ENTRIES(c))"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").indexFull().build(),
          "CREATE INDEX ON ks.tbl (FULL(c))"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").indexKeys().build(),
          "CREATE INDEX ON ks.tbl (KEYS(c))"),
      arguments(
          new QueryBuilder().create().index().on("ks", "tbl").column("c").indexValues().build(),
          "CREATE INDEX ON ks.tbl (VALUES(c))"),
      arguments(
          new QueryBuilder()
              .create()
              .index()
              .on("ks", "tbl")
              .column("c")
              .custom("IndexClass")
              .build(),
          "CREATE CUSTOM INDEX ON ks.tbl (c) USING 'IndexClass'"),
      arguments(
          new QueryBuilder().drop().index("idx").ifExists().build(), "DROP INDEX IF EXISTS idx"),
      arguments(new QueryBuilder().drop().index("ks", "idx").build(), "DROP INDEX ks.idx"),
      arguments(
          new QueryBuilder()
              .create()
              .materializedView("ks", "v")
              .asSelect()
              .column("a", Column.Kind.PARTITION_KEY)
              .column("b")
              .column("c")
              .from("ks", "tbl")
              .build(),
          "CREATE MATERIALIZED VIEW ks.v "
              + "AS SELECT a, b, c "
              + "FROM ks.tbl "
              + "WHERE a IS NOT NULL "
              + "AND b IS NOT NULL "
              + "AND c IS NOT NULL "
              + "PRIMARY KEY ((a))"),
      arguments(
          new QueryBuilder().drop().materializedView("ks", "tbl").build(),
          "DROP MATERIALIZED VIEW ks.tbl"),
      arguments(
          new QueryBuilder().drop().materializedView("ks", "tbl").ifExists().build(),
          "DROP MATERIALIZED VIEW IF EXISTS ks.tbl"),
      arguments(
          new QueryBuilder().create().type("ks", "t").column("a", "int").column("b", "int").build(),
          "CREATE TYPE ks.t(a int, b int)"),
      arguments(
          new QueryBuilder()
              .alter()
              .type("ks", "t")
              .renameColumn("a", "a2")
              .renameColumn("b", "b2")
              .build(),
          "ALTER TYPE ks.t RENAME a TO a2 AND b TO b2"),
      arguments(new QueryBuilder().drop().type("ks", "t").build(), "DROP TYPE ks.t"),
      arguments(
          new QueryBuilder().drop().type("ks", "t").ifExists().build(), "DROP TYPE IF EXISTS ks.t"),
      arguments(
          new QueryBuilder()
              .alter()
              .type("ks", "t")
              .addColumn("c", "int")
              .addColumn("d", "int")
              .build(),
          "ALTER TYPE ks.t ADD c int, d int"),
      arguments(
          new QueryBuilder()
              .insertInto("ks", "tbl")
              .value("a", 1)
              .value(ValueModifier.marker("b"))
              .build(),
          "INSERT INTO ks.tbl (a, b) VALUES (1, ?)"),
      arguments(
          new QueryBuilder()
              .insertInto("ks", "tbl")
              .value("a", "text")
              .value(ValueModifier.marker("b"))
              .ifNotExists()
              .ttl()
              .timestamp(1L)
              .build(),
          "INSERT INTO ks.tbl (a, b) VALUES ('text', ?) IF NOT EXISTS USING TTL ? AND TIMESTAMP 1"),
      arguments(
          new QueryBuilder()
              .update("ks", "tbl")
              .value("a")
              .value("b", "test")
              .value(
                  ValueModifier.of(
                      ValueModifier.Target.column("c"),
                      ValueModifier.Operation.PREPEND,
                      Value.marker()))
              .where("k", Predicate.EQ)
              .ifs("v", Predicate.GT)
              .ifExists()
              .build(),
          "UPDATE ks.tbl SET a = ?, "
              + "b = 'test', "
              + "c = ? + c "
              + "WHERE k = ? "
              + "IF EXISTS "
              + "AND v > ?"),
      arguments(
          new QueryBuilder()
              .delete()
              .column("a", "b", "c")
              .from("ks", "tbl")
              .where("k", Predicate.EQ)
              .ifs("v", Predicate.IN)
              .build(),
          "DELETE a, b, c FROM ks.tbl WHERE k = ? IF v IN ?"),
      arguments(new QueryBuilder().select().from("ks", "tbl").build(), "SELECT * FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().column("a", "b", "c").from("ks", "tbl").build(),
          "SELECT a, b, c FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").build(),
          "SELECT COUNT(a) FROM ks.tbl"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .from("ks", "tbl")
              .where("k", Predicate.EQ)
              .where("cc", Predicate.GT)
              .build(),
          "SELECT a, b, c FROM ks.tbl WHERE k = ? AND cc > ?"),
      arguments(
          new QueryBuilder()
              .select()
              .star()
              .from("ks", "tbl")
              .where("k", Predicate.GT)
              .groupBy("k")
              .groupBy("cc1")
              .groupBy("cc2")
              .orderBy("cc1", Column.Order.ASC)
              .orderBy("cc2", Column.Order.DESC)
              .build(),
          "SELECT * FROM ks.tbl WHERE k > ? GROUP BY k, cc1, cc2 ORDER BY cc1 ASC, cc2 DESC"),
    };
  }
}
