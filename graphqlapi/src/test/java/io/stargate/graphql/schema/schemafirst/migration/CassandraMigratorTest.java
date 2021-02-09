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
package io.stargate.graphql.schema.schemafirst.migration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.graphql.schema.SampleKeyspaces;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CassandraMigratorTest {

  private static final MigrationQuery CREATE_AUTHORS =
      new CreateTableQuery(SampleKeyspaces.LIBRARY.table("authors"));
  private static final MigrationQuery ADD_BOOKS_PAGES =
      new AddTableColumnQuery(
          SampleKeyspaces.LIBRARY.table("books"),
          ImmutableColumn.builder()
              .keyspace("library")
              .table("books")
              .name("pages")
              .type(Column.Type.Int)
              .build());
  private static final MigrationQuery ADD_BOOKS_EDITOR =
      new AddTableColumnQuery(
          SampleKeyspaces.LIBRARY.table("books"),
          ImmutableColumn.builder()
              .keyspace("library")
              .table("books")
              .name("editor")
              .type(Column.Type.Text)
              .build());

  private static final MigrationQuery CREATE_UDT_A =
      new CreateUdtQuery(SampleKeyspaces.UDTS.userDefinedType("A"));
  private static final MigrationQuery CREATE_UDT_B =
      new CreateUdtQuery(SampleKeyspaces.UDTS.userDefinedType("B"));
  private static final MigrationQuery CREATE_TEST_TABLE =
      new CreateTableQuery(SampleKeyspaces.UDTS.table("TestTable"));
  private static final MigrationQuery ADD_TEST_TABLE_B =
      new AddTableColumnQuery(
          SampleKeyspaces.UDTS.table("TestTable"),
          ImmutableColumn.builder()
              .keyspace("udts")
              .table("TestTable")
              .name("b")
              .type(SampleKeyspaces.UDTS.userDefinedType("B"))
              .build());
  private static final MigrationQuery DROP_UDT_A =
      new DropUdtQuery(SampleKeyspaces.UDTS.userDefinedType("A"));
  private static final MigrationQuery DROP_UDT_B =
      new DropUdtQuery(SampleKeyspaces.UDTS.userDefinedType("B"));
  private static final MigrationQuery DROP_TEST_TABLE =
      new DropTableQuery(SampleKeyspaces.UDTS.table("TestTable"));

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
}
