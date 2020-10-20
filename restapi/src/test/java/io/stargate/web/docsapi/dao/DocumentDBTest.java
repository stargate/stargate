package io.stargate.web.docsapi.dao;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.PreparedStatement.Bound;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Schema;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
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
    PreparedStatement stmt =
        documentDB.getInsertStatement("keyspace", "table", 1, new Object[0]).preparedStatement();
    assertThat(stmt.preparedQueryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?");
  }

  @Test
  public void getPrefixDeleteStatement() {
    PreparedStatement stmt =
        documentDB
            .getPrefixDeleteStatement("keyspace", "table", "k", 1, ImmutableList.of("a", "b", "c"))
            .preparedStatement();
    assertThat(stmt.preparedQueryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2");
  }

  @Test
  public void getSubpathArrayDeleteStatement() {
    PreparedStatement stmt =
        documentDB
            .getSubpathArrayDeleteStatement(
                "keyspace", "table", "k", 1, ImmutableList.of("a", "b", "c", "d"))
            .preparedStatement();
    assertThat(stmt.preparedQueryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 >= '[000000]' AND p4 <= '[999999]' ");
  }

  @Test
  public void getPathKeysDeleteStatement() {
    PreparedStatement stmt =
        documentDB
            .getPathKeysDeleteStatement(
                "keyspace",
                "table",
                "k",
                1,
                ImmutableList.of("a", "b", "c", "d", "e"),
                ImmutableList.of("a", "few", "keys"))
            .preparedStatement();
    assertThat(stmt.preparedQueryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 = :p4 AND p5 IN (:p5,:p6,:p7) ");
  }

  @Test
  public void getExactPathDeleteStatement() {
    PreparedStatement stmt =
        documentDB
            .getExactPathDeleteStatement(
                "keyspace", "table", "key", 1, ImmutableList.of("a", "b", "c", "d", "e"))
            .preparedStatement();
    String expected =
        "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 = :p4";

    for (int i = 5; i < 64; i++) {
      expected += " AND p" + i + " = ''";
    }
    assertThat(stmt.preparedQueryString()).isEqualTo(expected);
  }

  // Args are either "normal" value, Object[] or List<Object>. In the latter cases, we "inline" the
  // array/list.
  private static Object[] makeValues(Object... valuesOrArraysOfValues) {
    List<Object> allValues = new ArrayList<>();
    for (Object value : valuesOrArraysOfValues) {
      allValues.addAll(maybeUnpack(value));
    }
    return allValues.toArray();
  }

  private static List<Object> maybeUnpack(Object value) {
    if (value instanceof Object[]) return Arrays.asList((Object[]) value);
    if (value instanceof List) return (List) value;
    return Collections.singletonList(value);
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
    documentDB.deleteThenInsertBatch(
        "keyspace", "table", "key", Collections.singletonList(values), path, 1L);

    List<PreparedStatement.Bound> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2")
            .bind(makeValues(0L, "key", path)));
    expectedStmts.add(
        new TestPreparedStatement(
                "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?")
            .bind(makeValues(values, 1L)));

    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
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
        "keyspace", "table", "key", Collections.singletonList(values), path, patchedKeys, 1L);

    List<PreparedStatement.Bound> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
                "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?")
            .bind(makeValues(values, 1L)));
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = '' AND p4 = '' AND p5 = '' AND p6 = '' AND p7 = '' AND p8 = '' AND p9 = '' AND p10 = '' AND p11 = '' AND p12 = '' AND p13 = '' AND p14 = '' AND p15 = '' AND p16 = '' AND p17 = '' AND p18 = '' AND p19 = '' AND p20 = '' AND p21 = '' AND p22 = '' AND p23 = '' AND p24 = '' AND p25 = '' AND p26 = '' AND p27 = '' AND p28 = '' AND p29 = '' AND p30 = '' AND p31 = '' AND p32 = '' AND p33 = '' AND p34 = '' AND p35 = '' AND p36 = '' AND p37 = '' AND p38 = '' AND p39 = '' AND p40 = '' AND p41 = '' AND p42 = '' AND p43 = '' AND p44 = '' AND p45 = '' AND p46 = '' AND p47 = '' AND p48 = '' AND p49 = '' AND p50 = '' AND p51 = '' AND p52 = '' AND p53 = '' AND p54 = '' AND p55 = '' AND p56 = '' AND p57 = '' AND p58 = '' AND p59 = '' AND p60 = '' AND p61 = '' AND p62 = '' AND p63 = ''")
            .bind(makeValues(0L, "key", path)));
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 >= '[000000]' AND p3 <= '[999999]' ")
            .bind(makeValues(0L, "key", path)));
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 IN (:p3) ")
            .bind(makeValues(0L, "key", path, patchedKeys)));
    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
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

    List<PreparedStatement.Bound> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2")
            .bind(makeValues(1L, "key", path)));
    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
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

    List<PreparedStatement.Bound> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 IN (:p1) ")
            .bind(1L, "key", "a", "b"));
    expectedStmts.add(
        new TestPreparedStatement(
                "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 >= '[000000]' AND p1 <= '[999999]' ")
            .bind(1L, "key", "b"));
    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
  }

  private class TestPreparedStatement implements PreparedStatement {
    private String cql;
    private TestDataStore testDataStore;

    public TestPreparedStatement(String cql) {
      this(cql, null);
    }

    public TestPreparedStatement(String cql, TestDataStore testDataStore) {
      this.cql = cql;
      this.testDataStore = testDataStore;
    }

    public int hashCode() {
      return this.cql.hashCode();
    }

    public boolean equals(Object other) {
      return other instanceof TestPreparedStatement
          && ((TestPreparedStatement) other).cql.equals(this.cql);
    }

    @Override
    public String preparedQueryString() {
      return cql;
    }

    @Override
    public Bound bind(Object... values) {
      return new TestBound(values);
    }

    @Override
    public String toString() {
      return format("Prepared[%s]", this.cql);
    }

    private class TestBound implements Bound {
      private final Object[] values;

      TestBound(Object[] values) {
        this.values = values;
      }

      @Override
      public PreparedStatement preparedStatement() {
        return TestPreparedStatement.this;
      }

      @Override
      public List<Object> values() {
        return Arrays.asList(values);
      }

      @Override
      public CompletableFuture<ResultSet> execute(UnaryOperator<Parameters> parametersModifier) {
        if (testDataStore != null) {
          testDataStore.recentStatements.add(this);
        }
        return CompletableFuture.completedFuture(ResultSet.empty());
      }

      @Override
      public BoundStatement toPersistenceStatement(ProtocolVersion protocolVersion) {
        return null;
      }

      public int hashCode() {
        return Objects.hash(preparedStatement(), Objects.hash(values));
      }

      public boolean equals(Object other) {
        if (!(other instanceof TestBound)) return false;

        TestBound that = (TestBound) other;
        return this.preparedStatement().equals(that.preparedStatement())
            && Arrays.equals(this.values, that.values);
      }

      @Override
      public String toString() {
        return format("%s with values %s", preparedStatement(), Arrays.toString(values));
      }
    }
  }

  private class TestDataStore implements DataStore {
    private final List<PreparedStatement.Bound> recentStatements = new ArrayList<>();

    @Override
    public CompletableFuture<ResultSet> query(
        String queryString, UnaryOperator<Parameters> parametersModifier, Object... values) {
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    @Override
    public CompletableFuture<PreparedStatement> prepare(String s) {
      return CompletableFuture.completedFuture(new TestPreparedStatement(s, this));
    }

    @Override
    public CompletableFuture<ResultSet> batch(
        List<Bound> statements, BatchType batchType, UnaryOperator<Parameters> parametersModifier) {
      System.out.println(statements);
      this.recentStatements.addAll(statements);
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    @Override
    public CompletableFuture<ResultSet> batch(List<String> queries) {
      CompletableFuture<ResultSet> f = new CompletableFuture<>();
      f.completeExceptionally(
          new RuntimeException("Call to batch() was not faked in TestDataStore"));
      return f;
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

    public List<PreparedStatement.Bound> getRecentStatements() {
      List<PreparedStatement.Bound> recent = new ArrayList<>(this.recentStatements);
      this.recentStatements.clear();
      return recent;
    }
  }
}
