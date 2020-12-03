package io.stargate.web.docsapi.dao;

import static io.stargate.db.query.TypedValue.javaValues;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Schema;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.junit.Before;
import org.junit.Test;

public class DocumentDBTest {
  private DocumentDB documentDB;
  private TestDataStore ds;
  private static final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void setup() {
    ds = new TestDataStore();
    documentDB = new DocumentDB(ds);
  }

  @Test
  public void getForbiddenCharactersMessage() {
    List<String> res = DocumentDB.getForbiddenCharactersMessage();
    assertThat(res).isEqualTo(ImmutableList.of("`[`", "`]`", "`,`", "`.`", "`\'`", "`*`"));
  }

  @Test
  public void containsIllegalChars() {
    assertThat(DocumentDB.containsIllegalChars("[")).isTrue();
    assertThat(DocumentDB.containsIllegalChars("]")).isTrue();
    assertThat(DocumentDB.containsIllegalChars(",")).isTrue();
    assertThat(DocumentDB.containsIllegalChars(".")).isTrue();
    assertThat(DocumentDB.containsIllegalChars("\'")).isTrue();
    assertThat(DocumentDB.containsIllegalChars("*")).isTrue();
    assertThat(DocumentDB.containsIllegalChars("a[b")).isTrue();
    assertThat(DocumentDB.containsIllegalChars("\"")).isFalse();
  }

  @Test
  public void getInsertStatement() {
    BoundQuery query =
        documentDB.getInsertStatement(
            "keyspace", "table", 1, new Object[DocumentDB.allColumns().size()]);
    assertThat(query.queryString())
        .isEqualTo(
            "INSERT INTO keyspace.table(key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
  }

  @Test
  public void getPrefixDeleteStatement() {
    BoundQuery query =
        documentDB.getPrefixDeleteStatement(
            "keyspace", "table", "k", 1, ImmutableList.of("a", "b", "c"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ?");
    assertThat(javaValues(query.values())).isEqualTo(asList(1, "k", "a", "b", "c"));
  }

  @Test
  public void getSubpathArrayDeleteStatement() {
    BoundQuery query =
        documentDB.getSubpathArrayDeleteStatement(
            "keyspace", "table", "k", 1, ImmutableList.of("a", "b", "c", "d"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 >= ? AND p4 <= ? ");
    assertThat(javaValues(query.values()))
        .isEqualTo(asList(1, "k", "a", "b", "c", "d", "[000000]", "[999999]"));
  }

  @Test
  public void getPathKeysDeleteStatement() {
    BoundQuery query =
        documentDB.getPathKeysDeleteStatement(
            "keyspace",
            "table",
            "k",
            1,
            ImmutableList.of("a", "b", "c", "d", "e"),
            ImmutableList.of("a", "few", "keys"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM keyspace.table USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND p5 IN ?");
    assertThat(javaValues(query.values()))
        .isEqualTo(asList(1, "k", "a", "b", "c", "d", "e", ImmutableList.of("a", "few", "keys")));
  }

  @Test
  public void getExactPathDeleteStatement() {
    BoundQuery query =
        documentDB.getExactPathDeleteStatement(
            "keyspace", "table", "key", 1, ImmutableList.of("a", "b", "c", "d", "e"));
    StringBuilder expected =
        new StringBuilder("DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = ?");
    for (int i = 0; i < 64; i++) {
      expected.append(" AND p").append(i).append(" = ?");
    }
    assertThat(query.queryString()).isEqualTo(expected.toString());

    List<Object> expectedValues = new ArrayList<>();
    expectedValues.add(1);
    expectedValues.add("key");
    expectedValues.addAll(ImmutableList.of("a", "b", "c", "d", "e"));
    for (int i = 5; i < 64; i++) {
      expectedValues.add("");
    }
    assertThat(javaValues(query.values())).isEqualTo(expectedValues);
  }

  // Args are either "normal" value, Object[] or List<Object>. In the latter cases, we "inline" the
  // array/list.
  private static List<Object> makeValues(Object... valuesOrArraysOfValues) {
    List<Object> allValues = new ArrayList<>();
    for (Object value : valuesOrArraysOfValues) {
      allValues.addAll(maybeUnpack(value));
    }
    return allValues;
  }

  private static List<Object> maybeUnpack(Object value) {
    if (value instanceof Object[]) return asList((Object[]) value);
    if (value instanceof List) return (List) value;
    return singletonList(value);
  }

  @Test
  public void deleteThenInsertBatch() {
    ds = new TestDataStore();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    Map<String, Object> map = documentDB.newBindMap(path);
    map.put("bool_value", true);
    map.put("dbl_value", null);
    map.put("text_value", null);
    Object[] values = map.values().toArray();
    documentDB.deleteThenInsertBatch("keyspace", "table", "key", singletonList(values), path, 1L);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(2);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2");
    assertThat(javaValues(generatedQueries.get(0).values())).isEqualTo(makeValues(0L, "key", path));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
    assertThat(javaValues(generatedQueries.get(1).values())).isEqualTo(makeValues(values, 1L));
  }

  @Test
  public void deletePatchedPathsThenInsertBatch() {
    ds = new TestDataStore();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    List<String> patchedKeys = ImmutableList.of("eric");
    Map<String, Object> map = documentDB.newBindMap(path);
    map.put("bool_value", null);
    map.put("dbl_value", 3.0);
    map.put("text_value", null);
    Object[] values = map.values().toArray();
    documentDB.deletePatchedPathsThenInsertBatch(
        "keyspace", "table", "key", singletonList(values), path, patchedKeys, 1L);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(4);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
    assertThat(javaValues(generatedQueries.get(0).values())).isEqualTo(makeValues(values, 1L));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND p5 = ? AND p6 = ? AND p7 = ? AND p8 = ? AND p9 = ? AND p10 = ? AND p11 = ? AND p12 = ? AND p13 = ? AND p14 = ? AND p15 = ? AND p16 = ? AND p17 = ? AND p18 = ? AND p19 = ? AND p20 = ? AND p21 = ? AND p22 = ? AND p23 = ? AND p24 = ? AND p25 = ? AND p26 = ? AND p27 = ? AND p28 = ? AND p29 = ? AND p30 = ? AND p31 = ? AND p32 = ? AND p33 = ? AND p34 = ? AND p35 = ? AND p36 = ? AND p37 = ? AND p38 = ? AND p39 = ? AND p40 = ? AND p41 = ? AND p42 = ? AND p43 = ? AND p44 = ? AND p45 = ? AND p46 = ? AND p47 = ? AND p48 = ? AND p49 = ? AND p50 = ? AND p51 = ? AND p52 = ? AND p53 = ? AND p54 = ? AND p55 = ? AND p56 = ? AND p57 = ? AND p58 = ? AND p59 = ? AND p60 = ? AND p61 = ? AND p62 = ? AND p63 = ?");
    String[] emptyStrings = new String[61];
    Arrays.fill(emptyStrings, "");
    assertThat(javaValues(generatedQueries.get(1).values()))
        .isEqualTo(makeValues(0L, "key", path, emptyStrings));

    assertThat(generatedQueries.get(2).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 >= ? AND p3 <= ?");
    assertThat(javaValues(generatedQueries.get(2).values()))
        .isEqualTo(makeValues(0L, "key", path, "[000000]", "[999999]"));

    assertThat(generatedQueries.get(3).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 IN ?");
    assertThat(javaValues(generatedQueries.get(3).values()))
        .isEqualTo(makeValues(0L, "key", path, singletonList(patchedKeys)));
  }

  @Test
  public void delete() {
    ds = new TestDataStore();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    List<Object[]> vars = new ArrayList<>();
    vars.add(new Object[path.size() + 2]);
    vars.get(0)[0] = 1L;
    vars.get(0)[1] = "key";
    vars.get(0)[2] = "a";
    vars.get(0)[3] = "b";
    vars.get(0)[4] = "c";

    documentDB.delete("keyspace", "table", "key", path, 1L);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(1);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ?");
    assertThat(javaValues(generatedQueries.get(0).values())).isEqualTo(makeValues(1L, "key", path));
  }

  @Test
  public void deleteDeadLeaves() {
    ds = new TestDataStore();
    documentDB = new DocumentDB(ds);

    Map<String, List<JsonNode>> deadLeaves = new HashMap<>();
    deadLeaves.put("$.a", new ArrayList<>());
    ObjectNode objectNode = mapper.createObjectNode();
    objectNode.set("b", TextNode.valueOf("b"));
    deadLeaves.get("$.a").add(objectNode);

    ArrayNode arrayNode = mapper.createArrayNode();
    arrayNode.add(TextNode.valueOf("c"));
    deadLeaves.put("$.b", new ArrayList<>());
    deadLeaves.get("$.b").add(arrayNode);

    documentDB.deleteDeadLeaves("keyspace", "table", "key", 1L, deadLeaves);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(2);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "DELETE FROM keyspace.table USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 IN (?)");
    assertThat(javaValues(generatedQueries.get(0).values()))
        .isEqualTo(asList(1L, "key", "a", singletonList("b")));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "DELETE FROM keyspace.table USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 >= ? AND p1 <= ?");
    assertThat(javaValues(generatedQueries.get(1).values()))
        .isEqualTo(makeValues(1L, "key", "b", "[000000]", "[999999]"));
  }

  private static class TestDataStore implements DataStore {
    private final List<BoundQuery> recentQueries = new ArrayList<>();

    @Override
    public Codec valueCodec() {
      return Codec.testCodec();
    }

    @Override
    public <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query) {
      return CompletableFuture.completedFuture(query);
    }

    @Override
    public CompletableFuture<ResultSet> execute(
        BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
      this.recentQueries.add(query);
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    @Override
    public CompletableFuture<ResultSet> batch(
        Collection<BoundQuery> queries,
        BatchType batchType,
        UnaryOperator<Parameters> parametersModifier) {
      this.recentQueries.addAll(queries);
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    @Override
    public Schema schema() {
      return null;
    }

    @Override
    public boolean isInSchemaAgreement() {
      return true;
    }

    @Override
    public void waitForSchemaAgreement() {}

    public List<BoundQuery> getRecentStatements() {
      List<BoundQuery> recent = new ArrayList<>(this.recentQueries);
      this.recentQueries.clear();
      return recent;
    }
  }
}
