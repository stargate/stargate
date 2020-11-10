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
package io.stargate.web.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.db.datastore.query.ImmutableWhereCondition;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.datastore.query.WhereCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.web.service.WhereParser;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class WhereParserTest {

  @Test
  public void testParseSimple() throws IOException {
    String whereParam = "{ \"name\": {\"$eq\": \"Cliff\"} }";
    List<Where<?>> whereExpected =
        Collections.singletonList(
            ImmutableWhereCondition.builder()
                .value("Cliff")
                .predicate(WhereCondition.Predicate.Eq)
                .column(ImmutableColumn.create("name", Column.Type.Text))
                .build());

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    List<WhereCondition<?>> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseNotObject() {
    String whereParam = "[ \"name\" ]";
    List<Where<?>> whereExpected =
        Collections.singletonList(
            ImmutableWhereCondition.builder()
                .value("Cliff")
                .predicate(WhereCondition.Predicate.Eq)
                .column("name")
                .build());

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Was expecting a JSON object as input for where parameter.");
  }

  @Test
  public void testParseConditionNotObject() {
    String whereParam = "{ \"name\": \"Cliff\" }";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Entry for field name was expecting a JSON object as input.");
  }

  @Test
  public void testParseValueEmpty() {
    String whereParam = "{ \"name\": {\"$gt\": null} }";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Value entry for field name, operation $gt was expecting a value, but found null.");
  }

  @Test
  public void testParseExistsNumber() {
    String whereParam = "{ \"name\": {\"$exists\": 5} }";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("`exists` only supports the value `true`");
  }

  @Test
  public void testParseUnsupportedOp() {
    String whereParam = "{ \"name\": {\"$foo\": 5} }";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Operation $foo is not supported");
  }

  @Test
  public void testParse() throws IOException {
    String whereParam = "{\"price\": {\"$gt\": 600, \"$lt\": 600.05}}";
    List<Where<?>> whereExpected =
        Arrays.asList(
            ImmutableWhereCondition.builder()
                .value(600.0)
                .predicate(WhereCondition.Predicate.Gt)
                .column(ImmutableColumn.create("price", Column.Type.Double))
                .build(),
            ImmutableWhereCondition.builder()
                .value(600.05)
                .predicate(WhereCondition.Predicate.Lt)
                .column(ImmutableColumn.create("price", Column.Type.Double))
                .build());

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("price", Column.Type.Double))
            .build();

    List<WhereCondition<?>> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseMultiColumn() throws IOException {
    String whereParam =
        "{\"price\": {\"$gt\": 600, \"$lt\": 600.05}, \"id\": {\"$eq\": \"c72e7d29-3c67-4b60-8cf8-db439b2bf66c\"}}";
    List<Where<?>> whereExpected =
        Arrays.asList(
            ImmutableWhereCondition.builder()
                .value(600.0)
                .predicate(WhereCondition.Predicate.Gt)
                .column(ImmutableColumn.create("price", Column.Type.Double))
                .build(),
            ImmutableWhereCondition.builder()
                .value(600.05)
                .predicate(WhereCondition.Predicate.Lt)
                .column(ImmutableColumn.create("price", Column.Type.Double))
                .build(),
            ImmutableWhereCondition.builder()
                .value(UUID.fromString("c72e7d29-3c67-4b60-8cf8-db439b2bf66c"))
                .predicate(WhereCondition.Predicate.Eq)
                .column(ImmutableColumn.create("id", Column.Type.Uuid))
                .build());

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("price", Column.Type.Double))
            .addColumns(ImmutableColumn.create("id", Column.Type.Uuid))
            .build();

    List<WhereCondition<?>> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseInvalidJson() {
    String whereParam = "bad json";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(
            () -> {
              WhereParser.parseWhere(whereParam, table);
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Input provided is not valid json");
  }

  @Test
  public void testInOperation() throws IOException {
    String whereParam = "{\"name\":{\"$in\":[\"foo\",\"bar\",\"baz\"]}}";
    List<Where<?>> whereExpected =
        Collections.singletonList(
            ImmutableWhereCondition.builder()
                .value(Arrays.asList("foo", "bar", "baz"))
                .predicate(WhereCondition.Predicate.In)
                .column(ImmutableColumn.create("name", Column.Type.Text))
                .build());

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    List<WhereCondition<?>> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }
}
