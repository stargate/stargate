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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.CassandraSchemaHelper.ExtendedColumn;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CassandraMigratorTest {

  private static final MigrationQuery CREATE_AUTHORS =
      new CreateTableQuery("library", findTable(SampleKeyspaces.LIBRARY, "authors"));

  private static final MigrationQuery ADD_BOOKS_PAGES =
      new AddTableColumnQuery(
          "library",
          "books",
          new ExtendedColumn(
              "books",
              ColumnSpec.newBuilder().setName("pages").setType(TypeSpecs.INT).build(),
              Column.Kind.REGULAR));
  private static final MigrationQuery ADD_BOOKS_EDITOR =
      new AddTableColumnQuery(
          "library",
          "books",
          new ExtendedColumn(
              "books",
              ColumnSpec.newBuilder().setName("editor").setType(TypeSpecs.VARCHAR).build(),
              Column.Kind.REGULAR));

  private static final Udt UDT_A = findUdt(SampleKeyspaces.UDTS, "A");
  private static final Udt UDT_B = findUdt(SampleKeyspaces.UDTS, "B");
  private static final MigrationQuery CREATE_UDT_A = new CreateUdtQuery("udts", UDT_A);
  private static final MigrationQuery CREATE_UDT_B = new CreateUdtQuery("udts", UDT_B);
  private static final MigrationQuery CREATE_TEST_TABLE =
      new CreateTableQuery("udts", findTable(SampleKeyspaces.UDTS, "TestTable"));
  private static final MigrationQuery ADD_TEST_TABLE_B =
      new AddTableColumnQuery(
          "udts",
          "TestTable",
          new ExtendedColumn(
              "TestTable",
              ColumnSpec.newBuilder()
                  .setName("b")
                  .setType(TypeSpec.newBuilder().setUdt(UDT_B))
                  .build(),
              Column.Kind.REGULAR));
  private static final MigrationQuery DROP_UDT_A = new DropUdtQuery("udts", UDT_A);
  private static final MigrationQuery DROP_UDT_B = new DropUdtQuery("udts", UDT_B);
  private static final MigrationQuery DROP_TEST_TABLE =
      new DropTableQuery("udts", findTable(SampleKeyspaces.UDTS, "TestTable"));

  @Test
  @DisplayName("Should sort queries when there are less than two")
  public void sortZeroOrOneQuery() {
    assertThat(CassandraMigrator.sortForExecution(Collections.emptyList())).isEmpty();
    assertThat(CassandraMigrator.sortForExecution(ImmutableList.of(CREATE_AUTHORS)))
        .containsExactly(CREATE_AUTHORS);
  }

  @Test
  @DisplayName("Should sort UDT creations before their uses")
  public void sortUdtCreations() {
    assertThat(
            CassandraMigrator.sortForExecution(
                ImmutableList.of(CREATE_TEST_TABLE, CREATE_UDT_A, CREATE_UDT_B)))
        .containsExactly(CREATE_UDT_B, CREATE_UDT_A, CREATE_TEST_TABLE);
  }

  @Test
  @DisplayName("Should sort column additions first")
  public void sortAddColumnFirst() {
    assertThat(
            CassandraMigrator.sortForExecution(
                ImmutableList.of(CREATE_AUTHORS, ADD_BOOKS_PAGES, ADD_BOOKS_EDITOR)))
        .containsExactly(ADD_BOOKS_PAGES, ADD_BOOKS_EDITOR, CREATE_AUTHORS);
  }

  @Test
  @DisplayName("Should not sort column addition before creation of UDT it references")
  public void sortAddColumnAfterUdt() {
    // This is a bit contrived because TestTable already references B through A, but that doesn't
    // affect the test, so reuse TestTable for simplicity.
    assertThat(CassandraMigrator.sortForExecution(ImmutableList.of(ADD_TEST_TABLE_B, CREATE_UDT_B)))
        .containsExactly(CREATE_UDT_B, ADD_TEST_TABLE_B);
  }

  @Test
  @DisplayName("Should sort UDT drops before their uses")
  public void sortUdtDrops() {
    assertThat(
            CassandraMigrator.sortForExecution(
                ImmutableList.of(DROP_UDT_B, DROP_UDT_A, DROP_TEST_TABLE)))
        .containsExactly(DROP_TEST_TABLE, DROP_UDT_A, DROP_UDT_B);
  }

  @Test
  @DisplayName("Should sort drop and recreate operations in this order")
  public void sortDropAndRecreate() {
    assertThat(
            CassandraMigrator.sortForExecution(
                ImmutableList.of(CREATE_TEST_TABLE, CREATE_UDT_A, DROP_UDT_A, DROP_TEST_TABLE)))
        .containsExactly(DROP_TEST_TABLE, DROP_UDT_A, CREATE_UDT_A, CREATE_TEST_TABLE);
  }

  private static CqlTable findTable(CqlKeyspaceDescribe keyspace, String tableName) {
    return keyspace.getTablesList().stream()
        .filter(t -> t.getName().equals(tableName))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static Udt findUdt(CqlKeyspaceDescribe keyspace, String udtName) {
    return keyspace.getTypesList().stream()
        .filter(t -> t.getName().equals(udtName))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }
}
