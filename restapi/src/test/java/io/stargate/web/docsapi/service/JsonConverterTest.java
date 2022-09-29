package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.web.docsapi.service.json.DeadLeaf;
import io.stargate.web.docsapi.service.json.DeadLeafCollectorImpl;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeaf;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JsonConverterTest {
  private JsonConverter service;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void setup() {
    service = new JsonConverter(mapper, DocsApiConfiguration.DEFAULT);
  }

  @Test
  public void convertToJsonDoc_testDeadLeaves() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(false);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
    JsonNode result = service.convertToJsonDoc(initial, collector, false, false);

    assertThat(result.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\", \"g\": {\"h\": \"something\"}}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(collector.isEmpty()).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeSecondRowData(false));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, collector, false, false);

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
    assertThat(leaves.contains(DeadLeaf.ARRAYLEAF)).isTrue();
    leaves = deadLeaves.get("$.g.h");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeThirdRowData(false));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, collector, false, false);

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
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.d");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.f");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.g");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
  }

  @Test
  public void convertToJsonDoc_testDeadLeaves_numericBools() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(true);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
    JsonNode result = service.convertToJsonDoc(initial, collector, false, true);

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
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, collector, false, true);

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
    assertThat(leaves.contains(DeadLeaf.ARRAYLEAF)).isTrue();
    leaves = deadLeaves.get("$.g.h");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(makeThirdRowData(true));
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, collector, false, true);

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
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.d");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.f");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.g");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
  }

  @Test
  public void convertToJsonDoc_noOpBaseCases() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData(false);
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    JsonNode result = service.convertToJsonDoc(initial, false, false);

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
    JsonNode result = service.convertToJsonDoc(initial, false, false);

    assertThat(result.toString())
        .isEqualTo(mapper.readTree("{\"a\":{\"b\":{\"c\":{}}}}").toString());
  }

  public static List<Row> makeInitialRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data0 = new HashMap<>();
    Map<String, Object> data1 = new HashMap<>();
    Map<String, Object> data2 = new HashMap<>();
    Map<String, Object> data3 = new HashMap<>();
    Map<String, Object> data4 = new HashMap<>();
    data0.put("key", "1");
    data1.put("key", "1");
    data2.put("key", "1");
    data3.put("key", "1");
    data4.put("key", "1");

    data0.put("writetime(leaf)", 0L);
    data1.put("writetime(leaf)", 0L);
    data2.put("writetime(leaf)", 0L);
    data3.put("writetime(leaf)", 0L);
    data4.put("writetime(leaf)", 0L);

    data0.put("p0", "");
    data0.put("p1", "");
    data0.put("p2", "");
    data0.put("p3", "");
    data0.put("leaf", DocsApiConstants.ROOT_DOC_MARKER);

    data1.put("p0", "a");
    data1.put("p1", "b");
    data1.put("p2", "c");
    data1.put("bool_value", true);
    data1.put("p3", "");
    data1.put("leaf", "c");

    data2.put("p0", "d");
    data2.put("p1", "e");
    data2.put("p2", "[0]");
    data2.put("dbl_value", 3.0);
    data2.put("p3", "");
    data2.put("leaf", "[0]");

    data3.put("p0", "f");
    data3.put("text_value", "abc");
    data3.put("p1", "");
    data3.put("p2", "");
    data3.put("p3", "");
    data3.put("leaf", "f");

    data4.put("p0", "g");
    data4.put("p1", "h");
    data4.put("text_value", "something");
    data4.put("p2", "");
    data4.put("p3", "");
    data4.put("leaf", "h");

    rows.add(makeRow(data0, numericBooleans));
    rows.add(makeRow(data1, numericBooleans));
    rows.add(makeRow(data2, numericBooleans));
    rows.add(makeRow(data3, numericBooleans));
    rows.add(makeRow(data4, numericBooleans));

    return rows;
  }

  public static List<Row> makeSecondRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();
    data1.put("key", "1");
    data1.put("writetime(leaf)", 1L);
    data1.put("p0", "a");
    data1.put("p1", "b");
    data1.put("p2", "c");
    data1.put("p3", "d");
    data1.put("text_value", "replaced");
    data1.put("leaf", "d");
    data1.put("p4", "");
    rows.add(makeRow(data1, numericBooleans));

    Map<String, Object> data2 = new HashMap<>();
    data2.put("key", "1");
    data2.put("writetime(leaf)", 1L);
    data2.put("p0", "d");
    data2.put("p1", "e");
    data2.put("p2", "f");
    data2.put("p3", "g");
    data2.put("text_value", "replaced");
    data2.put("leaf", "g");
    data2.put("p4", "");
    rows.add(makeRow(data2, numericBooleans));

    Map<String, Object> data3 = new HashMap<>();
    data3.put("key", "1");
    data3.put("writetime(leaf)", 1L);
    data3.put("p0", "g");
    data3.put("p1", "h");
    data3.put("p2", "[0]");
    data3.put("p3", "");
    data3.put("text_value", "replaced");
    data3.put("leaf", "[0]");
    data3.put("p4", "");
    rows.add(makeRow(data3, numericBooleans));
    return rows;
  }

  public static List<Row> makeThirdRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();

    data1.put("key", "1");
    data1.put("writetime(leaf)", 2L);
    data1.put("p0", "[0]");
    data1.put("text_value", "replaced");
    data1.put("p1", "");
    data1.put("p2", "");
    data1.put("p3", "");
    data1.put("leaf", "[0]");

    rows.add(makeRow(data1, numericBooleans));
    return rows;
  }

  public static List<Row> makeMultipleReplacements() {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();

    data1.put("key", "1");
    data1.put("writetime(leaf)", 0L);
    data1.put("p0", "a");
    data1.put("p1", "[0]");
    data1.put("p2", "b");
    data1.put("p3", "c");
    data1.put("text_value", "initial");
    data1.put("p4", "");
    data1.put("leaf", "c");

    Map<String, Object> data2 = new HashMap<>();

    data2.put("key", "1");
    data2.put("writetime(leaf)", 1L);
    data2.put("p0", "a");
    data2.put("p1", "[0]");
    data2.put("p2", "");
    data2.put("p3", "");
    data2.put("text_value", "initial");
    data2.put("leaf", "a");

    Map<String, Object> data3 = new HashMap<>();

    data3.put("key", "1");
    data3.put("writetime(leaf)", 2L);
    data3.put("p0", "a");
    data3.put("p1", "[0]");
    data3.put("p2", "[0]");
    data3.put("dbl_value", 1.23);
    data3.put("p3", "");
    data3.put("leaf", "a");

    Map<String, Object> data4 = new HashMap<>();

    data4.put("key", "1");
    data4.put("writetime(leaf)", 3L);
    data4.put("p0", "a");
    data4.put("p1", "[0]");
    data4.put("p2", "c");
    data4.put("text_value", DocsApiConstants.EMPTY_ARRAY_MARKER);
    data4.put("p3", "");
    data4.put("leaf", "c");

    Map<String, Object> data5 = new HashMap<>();

    data5.put("key", "1");
    data5.put("writetime(leaf)", 4L);
    data5.put("p0", "a");
    data5.put("p1", "b");
    data5.put("p2", "c");
    data5.put("text_value", DocsApiConstants.EMPTY_OBJECT_MARKER);
    data5.put("p3", "");
    data5.put("leaf", "c");

    rows.add(makeRow(data1, false));
    rows.add(makeRow(data2, false));
    rows.add(makeRow(data3, false));
    rows.add(makeRow(data4, false));
    rows.add(makeRow(data5, false));
    return rows;
  }

  private static Row makeRow(Map<String, Object> data, boolean numericBooleans) {
    List<Column> columns =
        new ArrayList<>(
            Arrays.asList(
                DocsApiConstants.ALL_COLUMNS.apply(
                    DocsApiConfiguration.DEFAULT.getMaxDepth(), numericBooleans)));
    columns.add(Column.create("writetime(leaf)", Type.Bigint));
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    ProtocolVersion version = ProtocolVersion.DEFAULT;
    if (numericBooleans) {
      columns.replaceAll(
          col -> {
            if (col.name().equals("bool_value")) {
              return Column.create("bool_value", Type.Tinyint);
            }
            return col;
          });
    }

    for (Column column : columns) {
      Object v = data.get(column.name());
      if (column.name().equals("bool_value") && numericBooleans && v != null) {
        v = ((Boolean) v) ? (byte) 1 : (byte) 0;
      }
      values.add(v == null ? null : column.type().codec().encode(v, version));
    }
    return new ArrayListBackedRow(columns, values, version);
  }
}
