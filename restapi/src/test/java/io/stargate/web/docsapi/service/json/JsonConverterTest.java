package io.stargate.web.docsapi.service.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.service.DocumentServiceTest;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JsonConverterTest {
  private JsonConverter service;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void setup() {
    service = new JsonConverter();
  }

  @Test
  public void convertToJsonDoc_testDeadLeaves() throws JsonProcessingException {
    List<Row> initial = DocumentServiceTest.makeInitialRowData();
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
                .readTree("{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(collector.isEmpty()).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(DocumentServiceTest.makeSecondRowData());
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
                    "{\"a\": {\"b\": {\"c\": {\"d\": \"replaced\"}}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have 1 dead leaf, since $.a.b.c was changed from `true` to an object
    Map<String, List<JsonNode>> expected = new HashMap<>();
    List<JsonNode> list = new ArrayList<>();
    ObjectNode node = mapper.createObjectNode();
    node.set("", BooleanNode.valueOf(true));
    list.add(node);
    expected.put("$.a.b.c", list);
    Map<String, Set<DeadLeaf>> deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(1);
    assertThat(deadLeaves.containsKey("$.a.b.c")).isTrue();
    Set<DeadLeaf> leaves = deadLeaves.get("$.a.b.c");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(ImmutableDeadLeaf.builder().name("").build())).isTrue();

    collector = new DeadLeafCollectorImpl();
    initial.addAll(DocumentServiceTest.makeThirdRowData());
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

    // This state should have 3 dead branches representing keys a, d, and f, since everything was
    // blown away by the latest change
    deadLeaves = collector.getLeaves();
    assertThat(deadLeaves.keySet().size()).isEqualTo(3);
    assertThat(deadLeaves.containsKey("$.a")).isTrue();
    assertThat(deadLeaves.containsKey("$.d")).isTrue();
    assertThat(deadLeaves.containsKey("$.f")).isTrue();
    leaves = deadLeaves.get("$.a");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.d");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
    leaves = deadLeaves.get("$.f");
    assertThat(leaves.size()).isEqualTo(1);
    assertThat(leaves.contains(DeadLeaf.STARLEAF)).isTrue();
  }
}
