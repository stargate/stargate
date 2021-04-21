package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.datastore.Row;
import java.util.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JsonConverterServiceTest {
  private JsonConverterService service;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void setup() {
    service = new JsonConverterService();
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
    ImmutablePair<JsonNode, Map<String, List<JsonNode>>> result =
        service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString())
        .isEqualTo(
            mapper
                .readTree("{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(result.right).isEmpty();

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
    result = service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString())
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
    assertThat(result.right).isEqualTo(expected);

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
    result = service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString()).isEqualTo(mapper.readTree("[\"replaced\"]").toString());

    // This state should have 3 dead branches representing keys a, d, and f, since everything was
    // blown away by the latest change
    expected = new HashMap<>();
    list = new ArrayList<>();
    node = mapper.createObjectNode();
    node.set("a", NullNode.getInstance());
    node.set("d", NullNode.getInstance());
    node.set("f", NullNode.getInstance());

    list.add(node);
    expected.put("$", list);

    assertThat(result.right).isEqualTo(expected);
  }
}
