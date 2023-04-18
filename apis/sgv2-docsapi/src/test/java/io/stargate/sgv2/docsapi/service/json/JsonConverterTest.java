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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(JsonConverterTest.Profile.class)
public class JsonConverterTest {

  public static class Profile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder().put("stargate.document.max-depth", "6").build();
    }
  }

  @Inject ObjectMapper mapper;

  @Inject JsonConverter service;

  @Test
  public void convertToJsonDoc_testDeadLeaves() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(false);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
    List<RowWrapper> rowWrappers = createRowWrappers(initial, columns());
    JsonNode result = service.convertToJsonDoc(rowWrappers, collector, false, false);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\", \"g\": {\"h\": \"something\"}}")
                .toString());

    assertThat(collector.isEmpty()).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeSecondRowData(false));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    rowWrappers = createRowWrappers(initial, columns());
    result = service.convertToJsonDoc(rowWrappers, collector, false, false);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\":{\"b\":{\"c\":{\"d\":\"replaced\"}}},\"d\":{\"e\":{\"f\":{\"g\":\"replaced\"}}},\"f\":\"abc\",\"g\":{\"h\": [\"replaced\"]}}")
                .toString());

    // This state should have 3 dead leaves, since $.a.b.c was changed from `true` to an object,
    // $.d.e was changed from an array to an object,
    // and $.g.h was changed to an array.
    Map<String, List<JsonNode>> expected = new HashMap<>();
    List<JsonNode> list = new ArrayList<>();
    ObjectNode node = mapper.createObjectNode();
    node.set("", BooleanNode.valueOf(true));
    list.add(node);
    expected.put("$.a.b.c", list);
    Map<String, Set<DeadLeaf>> deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(3);
    assertThat(deadLeaves.containsKey("$.a.b.c")).isTrue();
    assertThat(deadLeaves.containsKey("$.d.e")).isTrue();
    assertThat(deadLeaves.containsKey("$.g.h")).isTrue();
    Set<DeadLeaf> leaves = deadLeaves.get("$.a.b.c");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();
    leaves = deadLeaves.get("$.d.e");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.ARRAY_LEAF)).isTrue();
    leaves = deadLeaves.get("$.g.h");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeThirdRowData(false));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    rowWrappers = createRowWrappers(initial, columns());
    result = service.convertToJsonDoc(rowWrappers, collector, false, false);

    assertThat(result.toString()).isEqualTo(mapper.readTree("[\"replaced\"]").toString());

    // This state should have 4 dead branches representing keys a, d, f, and g, since everything was
    // blown away by the latest change
    deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(4);
    assertThat(deadLeaves.containsKey("$.a")).isTrue();
    assertThat(deadLeaves.containsKey("$.d")).isTrue();
    assertThat(deadLeaves.containsKey("$.f")).isTrue();
    assertThat(deadLeaves.containsKey("$.g")).isTrue();
    leaves = deadLeaves.get("$.a");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.d");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.f");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.g");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
  }

  @Test
  public void convertToJsonDoc_testDeadLeaves_numericBools() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(true);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
    List<RowWrapper> rowWrappers = createRowWrappers(initial, columns());
    JsonNode result = service.convertToJsonDoc(rowWrappers, collector, false, true);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\",\"g\":{\"h\":\"something\"}}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(collector.isEmpty()).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeSecondRowData(true));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    rowWrappers = createRowWrappers(initial, columns());
    result = service.convertToJsonDoc(rowWrappers, collector, false, true);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\":{\"b\":{\"c\":{\"d\":\"replaced\"}}},\"d\":{\"e\":{\"f\":{\"g\":\"replaced\"}}},\"f\":\"abc\",\"g\":{\"h\":[\"replaced\"]}}")
                .toString());

    // This state should have 3 dead leaves, since $.a.b.c was changed from `true` to an object,
    // $.d.e was changed from an array to an object,
    // and $.g.h was changed to an array.
    Map<String, List<JsonNode>> expected = new HashMap<>();
    List<JsonNode> list = new ArrayList<>();
    ObjectNode node = mapper.createObjectNode();
    node.set("", BooleanNode.valueOf(true));
    list.add(node);
    expected.put("$.a.b.c", list);
    Map<String, Set<DeadLeaf>> deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(3);
    assertThat(deadLeaves.containsKey("$.a.b.c")).isTrue();
    assertThat(deadLeaves.containsKey("$.d.e")).isTrue();
    assertThat(deadLeaves.containsKey("$.g.h")).isTrue();
    Set<DeadLeaf> leaves = deadLeaves.get("$.a.b.c");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();
    leaves = deadLeaves.get("$.d.e");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.ARRAY_LEAF)).isTrue();
    leaves = deadLeaves.get("$.g.h");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeThirdRowData(true));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    rowWrappers = createRowWrappers(initial, columns());
    result = service.convertToJsonDoc(rowWrappers, collector, false, true);

    assertThat(result.toString()).isEqualTo(mapper.readTree("[\"replaced\"]").toString());

    // This state should have 3 dead branches representing keys a, d, and f, since everything was
    // blown away by the latest change
    deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(4);
    assertThat(deadLeaves.containsKey("$.a")).isTrue();
    assertThat(deadLeaves.containsKey("$.d")).isTrue();
    assertThat(deadLeaves.containsKey("$.f")).isTrue();
    assertThat(deadLeaves.containsKey("$.g")).isTrue();
    leaves = deadLeaves.get("$.a");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.d");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.f");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
    leaves = deadLeaves.get("$.g");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STAR_LEAF)).isTrue();
  }

  @Test
  public void convertToJsonDoc_noOpBaseCases() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(false);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getValues(indexOfCol("p0")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p0")).getString()))
                    * 100000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p1")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p1")).getString()))
                    * 10000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p2")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p2")).getString()))
                    * 1000
                + Objects.requireNonNull(row1.getValues(indexOfCol("p3")).getString())
                        .compareTo(
                            Objects.requireNonNull(row2.getValues(indexOfCol("p3")).getString()))
                    * 100));
    List<RowWrapper> rowWrappers = createRowWrappers(initial, columns());
    JsonNode result = service.convertToJsonDoc(rowWrappers, false, false);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\",\"g\":{\"h\":\"something\"}}")
                .toString());

    result = service.convertToJsonDoc(new ArrayList<>(), false, false);
    assertThat(result.toString()).isEqualTo(mapper.readTree("{}").toString());
  }

  @Test
  public void convertToJsonDoc_arrayConversion() throws JsonProcessingException {
    List<Row> initial = makeMultipleReplacements();
    List<RowWrapper> rowWrappers = createRowWrappers(initial, columns());
    JsonNode result = service.convertToJsonDoc(rowWrappers, false, false);

    assertThat(result.toString())
        .isEqualTo(mapper.readTree("{\"a\":{\"b\":{\"c\":{}}}}").toString());
  }

  public static List<Row> makeInitialRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Row.Builder data0 = Row.newBuilder();
    Row.Builder data1 = Row.newBuilder();
    Row.Builder data2 = Row.newBuilder();
    Row.Builder data3 = Row.newBuilder();
    Row.Builder data4 = Row.newBuilder();

    // key
    data0.addValues(Value.newBuilder().setString("1").build());
    data1.addValues(Value.newBuilder().setString("1").build());
    data2.addValues(Value.newBuilder().setString("1").build());
    data3.addValues(Value.newBuilder().setString("1").build());
    data4.addValues(Value.newBuilder().setString("1").build());

    // writetime(leaf)
    data0.addValues(Value.newBuilder().setInt(0L).build());
    data1.addValues(Value.newBuilder().setInt(0L).build());
    data2.addValues(Value.newBuilder().setInt(0L).build());
    data3.addValues(Value.newBuilder().setInt(0L).build());
    data4.addValues(Value.newBuilder().setInt(0L).build());

    // paths for data0
    data0.addValues(Value.newBuilder().setString("").build());
    data0.addValues(Value.newBuilder().setString("").build());
    data0.addValues(Value.newBuilder().setString("").build());
    data0.addValues(Value.newBuilder().setString("").build());
    data0.addValues(Value.newBuilder().setString("").build());
    data0.addValues(Value.newBuilder().setString("").build());

    // values for data0
    data0.addValues(Value.newBuilder().setString(Constants.ROOT_DOC_MARKER).build());
    data0.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data0.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data0.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());

    // paths for data1
    data1.addValues(Value.newBuilder().setString("a").build());
    data1.addValues(Value.newBuilder().setString("b").build());
    data1.addValues(Value.newBuilder().setString("c").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());

    // values for data1
    data1.addValues(Value.newBuilder().setString("c").build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    if (numericBooleans) {
      data1.addValues(Value.newBuilder().setInt(1).build());
    } else {
      data1.addValues(Value.newBuilder().setBoolean(true).build());
    }
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());

    // paths for data2
    data2.addValues(Value.newBuilder().setString("d").build());
    data2.addValues(Value.newBuilder().setString("e").build());
    data2.addValues(Value.newBuilder().setString("[0]").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());

    // values for data2
    data2.addValues(Value.newBuilder().setString("[0]").build());
    data2.addValues(Value.newBuilder().setDouble(3.0).build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());

    // paths for data3
    data3.addValues(Value.newBuilder().setString("f").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());

    // values for data3
    data3.addValues(Value.newBuilder().setString("f").build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data3.addValues(Value.newBuilder().setString("abc").build());

    // paths for data4
    data4.addValues(Value.newBuilder().setString("g").build());
    data4.addValues(Value.newBuilder().setString("h").build());
    data4.addValues(Value.newBuilder().setString("").build());
    data4.addValues(Value.newBuilder().setString("").build());
    data4.addValues(Value.newBuilder().setString("").build());
    data4.addValues(Value.newBuilder().setString("").build());

    // values for data4
    data4.addValues(Value.newBuilder().setString("h").build());
    data4.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data4.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data4.addValues(Value.newBuilder().setString("something").build());

    rows.add(data0.build());
    rows.add(data1.build());
    rows.add(data2.build());
    rows.add(data3.build());
    rows.add(data4.build());

    return rows;
  }

  public static List<Row> makeSecondRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Row.Builder data1 = Row.newBuilder();
    data1.addValues(Value.newBuilder().setString("1").build());
    data1.addValues(Value.newBuilder().setInt(1L).build());
    data1.addValues(Value.newBuilder().setString("a").build());
    data1.addValues(Value.newBuilder().setString("b").build());
    data1.addValues(Value.newBuilder().setString("c").build());
    data1.addValues(Value.newBuilder().setString("d").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("d").build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setString("replaced").build());
    rows.add(data1.build());

    Row.Builder data2 = Row.newBuilder();
    data2.addValues(Value.newBuilder().setString("1").build());
    data2.addValues(Value.newBuilder().setInt(1L).build());
    data2.addValues(Value.newBuilder().setString("d").build());
    data2.addValues(Value.newBuilder().setString("e").build());
    data2.addValues(Value.newBuilder().setString("f").build());
    data2.addValues(Value.newBuilder().setString("g").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("g").build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data2.addValues(Value.newBuilder().setString("replaced").build());
    rows.add(data2.build());

    Row.Builder data3 = Row.newBuilder();
    data3.addValues(Value.newBuilder().setString("1").build());
    data3.addValues(Value.newBuilder().setInt(1L).build());
    data3.addValues(Value.newBuilder().setString("g").build());
    data3.addValues(Value.newBuilder().setString("h").build());
    data3.addValues(Value.newBuilder().setString("[0]").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("[0]").build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data3.addValues(Value.newBuilder().setString("replaced").build());
    rows.add(data3.build());
    return rows;
  }

  public static List<Row> makeThirdRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Row.Builder data1 = Row.newBuilder();

    data1.addValues(Value.newBuilder().setString("1").build());
    data1.addValues(Value.newBuilder().setInt(2L).build());

    data1.addValues(Value.newBuilder().setString("[0]").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());

    data1.addValues(Value.newBuilder().setString("[0]").build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setString("replaced").build());

    rows.add(data1.build());
    return rows;
  }

  public static List<Row> makeMultipleReplacements() {
    List<Row> rows = new ArrayList<>();
    Row.Builder data1 = Row.newBuilder();

    data1.addValues(Value.newBuilder().setString("1").build());
    data1.addValues(Value.newBuilder().setInt(0L).build());

    data1.addValues(Value.newBuilder().setString("a").build());
    data1.addValues(Value.newBuilder().setString("[0]").build());
    data1.addValues(Value.newBuilder().setString("b").build());
    data1.addValues(Value.newBuilder().setString("c").build());
    data1.addValues(Value.newBuilder().setString("").build());
    data1.addValues(Value.newBuilder().setString("").build());

    data1.addValues(Value.newBuilder().setString("c").build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data1.addValues(Value.newBuilder().setString("initial").build());

    Row.Builder data2 = Row.newBuilder();

    data2.addValues(Value.newBuilder().setString("1").build());
    data2.addValues(Value.newBuilder().setInt(1L).build());

    data2.addValues(Value.newBuilder().setString("a").build());
    data2.addValues(Value.newBuilder().setString("[0]").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());
    data2.addValues(Value.newBuilder().setString("").build());

    data2.addValues(Value.newBuilder().setString("a").build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data2.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data2.addValues(Value.newBuilder().setString("initial").build());

    Row.Builder data3 = Row.newBuilder();

    data3.addValues(Value.newBuilder().setString("1").build());
    data3.addValues(Value.newBuilder().setInt(2L).build());

    data3.addValues(Value.newBuilder().setString("a").build());
    data3.addValues(Value.newBuilder().setString("[0]").build());
    data3.addValues(Value.newBuilder().setString("[0]").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());
    data3.addValues(Value.newBuilder().setString("").build());

    data3.addValues(Value.newBuilder().setString("a").build());
    data3.addValues(Value.newBuilder().setDouble(1.23).build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data3.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());

    Row.Builder data4 = Row.newBuilder();

    data4.addValues(Value.newBuilder().setString("1").build());
    data4.addValues(Value.newBuilder().setInt(3L).build());

    data4.addValues(Value.newBuilder().setString("a").build());
    data4.addValues(Value.newBuilder().setString("[0]").build());
    data4.addValues(Value.newBuilder().setString("c").build());
    data4.addValues(Value.newBuilder().setString("").build());
    data4.addValues(Value.newBuilder().setString("").build());
    data4.addValues(Value.newBuilder().setString("").build());

    data4.addValues(Value.newBuilder().setString("c").build());
    data4.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data4.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data4.addValues(Value.newBuilder().setString(Constants.EMPTY_ARRAY_MARKER).build());

    Row.Builder data5 = Row.newBuilder();

    data5.addValues(Value.newBuilder().setString("1").build());
    data5.addValues(Value.newBuilder().setInt(4L).build());

    data5.addValues(Value.newBuilder().setString("a").build());
    data5.addValues(Value.newBuilder().setString("b").build());
    data5.addValues(Value.newBuilder().setString("c").build());
    data5.addValues(Value.newBuilder().setString("").build());
    data5.addValues(Value.newBuilder().setString("").build());
    data5.addValues(Value.newBuilder().setString("").build());

    data5.addValues(Value.newBuilder().setString("c").build());
    data5.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data5.addValues(Value.newBuilder().setNull(Value.Null.getDefaultInstance()).build());
    data5.addValues(Value.newBuilder().setString(Constants.EMPTY_OBJECT_MARKER).build());

    rows.add(data1.build());
    rows.add(data2.build());
    rows.add(data3.build());
    rows.add(data4.build());
    rows.add(data5.build());
    return rows;
  }

  private static List<ColumnSpec> columns() {
    List<String> columnNames = new ArrayList<>();
    columnNames.add("key");
    columnNames.add("writetime(leaf)");
    columnNames.add("p0");
    columnNames.add("p1");
    columnNames.add("p2");
    columnNames.add("p3");
    columnNames.add("p4");
    columnNames.add("p5");
    columnNames.add("leaf");
    columnNames.add("dbl_value");
    columnNames.add("bool_value");
    columnNames.add("text_value");

    return columnNames.stream()
        .map(
            colName -> {
              return ColumnSpec.newBuilder().setName(colName).build();
            })
        .collect(Collectors.toList());
  }

  public int indexOfCol(String colName) {
    List<ColumnSpec> cols = columns();
    for (int i = 0; i < cols.size(); i++) {
      ColumnSpec c = cols.get(i);
      if (c.getName().equals(colName)) {
        return i;
      }
    }
    return -1;
  }

  public List<RowWrapper> createRowWrappers(List<Row> rows, List<ColumnSpec> columns) {
    List<RowWrapper> rowWrappers = new ArrayList<>();
    Function<Row, RowWrapper> wrapper = RowWrapper.forColumns(columns);
    for (Row r : rows) {
      RowWrapper rowWrapper = wrapper.apply(r);
      rowWrappers.add(rowWrapper);
    }
    return rowWrappers;
  }
}
