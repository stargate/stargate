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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.web.service.WhereParser;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class WhereParserTest {

  @Test
  public void testParseSimple() throws IOException {
    String whereParam = "{ \"name\": {\"$eq\": \"Cliff\"} }";
    List<BuiltCondition> whereExpected =
        singletonList(BuiltCondition.of("name", Predicate.EQ, "Cliff"));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseUppercaseColumn() throws IOException {
    String whereParam = "{ \"Name\": {\"$eq\": \"Cliff\"} }";
    List<BuiltCondition> whereExpected =
        singletonList(BuiltCondition.of("Name", Predicate.EQ, "Cliff"));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("Name", Column.Type.Text))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseNotObject() {
    String whereParam = "[ \"name\" ]";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
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

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
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

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Value entry for field name, operation $gt was expecting a value, but found null.");
  }

  // 04-Jan-2022, tatu: Verifies existing behavior as used by Stargate REST 1.0,
  //   which seems to differ from Documents API.
  @Test
  public void testParseExistsSimple() throws Exception {
    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(
                ImmutableColumn.create("name", Type.Text),
                ImmutableColumn.create("enabled", Type.Boolean))
            .build();
    List<BuiltCondition> whereActual =
        WhereParser.parseWhere("{ \"enabled\": {\"$exists\": true} }", table);
    List<BuiltCondition> whereExpected =
        singletonList(BuiltCondition.of("enabled", Predicate.EQ, Boolean.TRUE));

    assertThat(whereActual).isEqualTo(whereExpected);
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

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
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

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Operation $foo is not supported");
  }

  @Test
  public void testParseInvalidFieldName() {
    String whereParam = "{ \"invalid_field\": {\"$eq\": 10} }";

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown field name 'invalid_field'");
  }

  @Test
  public void testParse() throws IOException {
    String whereParam = "{\"price\": {\"$gt\": 600, \"$lt\": 600.05}}";
    List<BuiltCondition> whereExpected =
        asList(
            BuiltCondition.of("price", Predicate.GT, 600.0),
            BuiltCondition.of("price", Predicate.LT, 600.05));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("price", Column.Type.Double))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testDuplicateJsonKey() {
    String whereParam = "{\"text\": {\"$eq\": \"a\", \"$eq\": \"b\"}}";
    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("text", Column.Type.Text))
            .build();

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate field '$eq'");
  }

  @Test
  public void testParseMap() throws IOException {
    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(
                ImmutableColumn.create("text", Column.Type.Text),
                ImmutableColumn.create("set", Column.Type.Set.of(Column.Type.Int)),
                ImmutableColumn.create(
                    "text_map", Column.Type.Map.of(Column.Type.Text, Column.Type.Text)))
            .build();

    String whereParam = "{\"text_map\": {\"$contains\": [\"a\", \"b\"]}}";
    List<BuiltCondition> whereExpected =
        asList(
            BuiltCondition.of("text_map", Predicate.CONTAINS, "a"),
            BuiltCondition.of("text_map", Predicate.CONTAINS, "b"));
    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);
    assertThat(where).isEqualTo(whereExpected);

    whereParam = "{\"set\": {\"$contains\": [76, 1]}}";
    whereExpected =
        asList(
            BuiltCondition.of("set", Predicate.CONTAINS, 76),
            BuiltCondition.of("set", Predicate.CONTAINS, 1));
    where = WhereParser.parseWhere(whereParam, table);
    assertThat(where).isEqualTo(whereExpected);

    whereParam = "{\"text_map\": {\"$containskey\": [\"a\", \"b\"]}}";
    whereExpected =
        asList(
            BuiltCondition.of("text_map", Predicate.CONTAINS_KEY, "a"),
            BuiltCondition.of("text_map", Predicate.CONTAINS_KEY, "b"));
    where = WhereParser.parseWhere(whereParam, table);
    assertThat(where).isEqualTo(whereExpected);

    whereParam =
        "{\"text_map\": {\"$containsentry\": [{\"key\": \"a\", \"value\": \"1\"}, {\"key\": \"b\", \"value\": \"2\"}]}}";

    whereExpected =
        asList(
            BuiltCondition.of(BuiltCondition.LHS.mapAccess("text_map", "a"), Predicate.EQ, "1"),
            BuiltCondition.of(BuiltCondition.LHS.mapAccess("text_map", "b"), Predicate.EQ, "2"));
    where = WhereParser.parseWhere(whereParam, table);
    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testParseMultiColumn() throws IOException {
    String whereParam =
        "{\"price\": {\"$gt\": 600, \"$lt\": 600.05}, \"id\": {\"$eq\": \"c72e7d29-3c67-4b60-8cf8-db439b2bf66c\"}}";
    List<BuiltCondition> whereExpected =
        asList(
            BuiltCondition.of("price", Predicate.GT, 600.0),
            BuiltCondition.of("price", Predicate.LT, 600.05),
            BuiltCondition.of(
                "id", Predicate.EQ, UUID.fromString("c72e7d29-3c67-4b60-8cf8-db439b2bf66c")));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("price", Column.Type.Double))
            .addColumns(ImmutableColumn.create("id", Column.Type.Uuid))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

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

    assertThatThrownBy(() -> WhereParser.parseWhere(whereParam, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Input provided is not valid json");
  }

  @Test
  public void testInOperation() throws IOException {
    String whereParam = "{\"name\":{\"$in\":[\"foo\",\"bar\",\"baz\"]}}";
    List<BuiltCondition> whereExpected =
        singletonList(BuiltCondition.of("name", Predicate.IN, asList("foo", "bar", "baz")));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("name", Column.Type.Text))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }

  @Test
  public void testInWithTimestampOperation() throws IOException {
    String whereParam = "{\"created\":{\"$in\":[\"2021-04-23T18:42:22.139Z\"]}}";
    List<BuiltCondition> whereExpected =
        singletonList(
            BuiltCondition.of(
                "created",
                Predicate.IN,
                singletonList(Converters.toCqlValue(Type.Timestamp, "2021-04-23T18:42:22.139Z"))));

    ImmutableTable table =
        ImmutableTable.builder()
            .name("table")
            .keyspace("keyspace")
            .addColumns(ImmutableColumn.create("created", Type.Timestamp))
            .build();

    List<BuiltCondition> where = WhereParser.parseWhere(whereParam, table);

    assertThat(where).isEqualTo(whereExpected);
  }
}
