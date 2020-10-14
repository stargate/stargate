package io.stargate.web.docsapi.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
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
    TestPreparedStatement stmt =
        (TestPreparedStatement) documentDB.getInsertStatement("keyspace", "table");
    assertThat(stmt.getCql())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?");
  }

  @Test
  public void getPrefixDeleteStatement() {
    TestPreparedStatement stmt =
        (TestPreparedStatement)
            documentDB.getPrefixDeleteStatement(
                "keyspace", "table", ImmutableList.of("a", "b", "c"));
    assertThat(stmt.getCql())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2");
  }

  @Test
  public void getSubpathArrayDeleteStatement() {
    TestPreparedStatement stmt =
        (TestPreparedStatement)
            documentDB.getSubpathArrayDeleteStatement(
                "keyspace", "table", ImmutableList.of("a", "b", "c", "d"));
    assertThat(stmt.getCql())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 >= '[000000]' AND p4 <= '[999999]' ");
  }

  @Test
  public void getPathKeysDeleteStatement() {
    TestPreparedStatement stmt =
        (TestPreparedStatement)
            documentDB.getPathKeysDeleteStatement(
                "keyspace",
                "table",
                ImmutableList.of("a", "b", "c", "d", "e"),
                ImmutableList.of("a", "few", "keys"));
    assertThat(stmt.getCql())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 = :p4 AND p5 IN (:p5,:p5,:p5) ");
  }

  @Test
  public void getExactPathDeleteStatement() {
    TestPreparedStatement stmt =
        (TestPreparedStatement)
            documentDB.getExactPathDeleteStatement(
                "keyspace", "table", ImmutableList.of("a", "b", "c", "d", "e"));
    String expected =
        "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = :p3 AND p4 = :p4";

    for (int i = 5; i < 64; i++) {
      expected += " AND p" + i + " = ''";
    }
    assertThat(stmt.getCql()).isEqualTo(expected);
  }

  @Test
  public void getRootDocInsertStatement() {
    TestPreparedStatement stmt =
        (TestPreparedStatement) documentDB.getRootDocInsertStatement("keyspace", "table", true);
    String allPaths = "";
    for (int i = 0; i < 64; i++) {
      allPaths += "p" + i + ", ";
    }

    String allPathValues = "";
    for (int i = 0; i < 64; i++) {
      allPathValues += "'', ";
    }

    assertThat(stmt.getCql())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, "
                + allPaths
                + "leaf) VALUES (:key, "
                + allPathValues
                + "'DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900') IF NOT EXISTS");

    stmt = (TestPreparedStatement) documentDB.getRootDocInsertStatement("keyspace", "table", false);

    assertThat(stmt.getCql())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, "
                + allPaths
                + "leaf) VALUES (:key, "
                + allPathValues
                + "'DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900') USING TIMESTAMP ?");
  }

  @Test
  public void deleteThenInsertBatch() {
    ds = mock(TestDataStore.class);
    when(ds.prepare(anyString())).thenCallRealMethod();
    when(ds.processBatch(anyObject(), anyList(), anyObject())).thenCallRealMethod();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    Map<String, Object> map = documentDB.newBindMap(path, 1L);
    map.put("bool_value", true);
    map.put("dbl_value", null);
    map.put("text_value", null);
    List<Object[]> vars = new ArrayList<>();
    vars.add(map.values().toArray());
    documentDB.deleteThenInsertBatch("keyspace", "table", "key", vars, path, 1L);

    List<PreparedStatement> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2"));
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf) VALUES (:key, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900') USING TIMESTAMP ?"));
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?"));

    verify(ds, times(1))
        .processBatch(expectedStmts, vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM));
  }

  @Test
  public void deletePatchedPathsThenInsertBatch() {
    ds = mock(TestDataStore.class);
    when(ds.prepare(anyString())).thenCallRealMethod();
    when(ds.processBatch(anyObject(), anyList(), anyObject())).thenCallRealMethod();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    List<String> patchedKeys = ImmutableList.of("eric");
    Map<String, Object> map = documentDB.newBindMap(path, 1L);
    map.put("bool_value", null);
    map.put("dbl_value", 3.0);
    map.put("text_value", null);
    List<Object[]> vars = new ArrayList<>();
    vars.add(map.values().toArray());
    documentDB.deletePatchedPathsThenInsertBatch(
        "keyspace", "table", "key", vars, path, patchedKeys, 1L);

    List<PreparedStatement> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?"));
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ?  WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 = '' AND p4 = '' AND p5 = '' AND p6 = '' AND p7 = '' AND p8 = '' AND p9 = '' AND p10 = '' AND p11 = '' AND p12 = '' AND p13 = '' AND p14 = '' AND p15 = '' AND p16 = '' AND p17 = '' AND p18 = '' AND p19 = '' AND p20 = '' AND p21 = '' AND p22 = '' AND p23 = '' AND p24 = '' AND p25 = '' AND p26 = '' AND p27 = '' AND p28 = '' AND p29 = '' AND p30 = '' AND p31 = '' AND p32 = '' AND p33 = '' AND p34 = '' AND p35 = '' AND p36 = '' AND p37 = '' AND p38 = '' AND p39 = '' AND p40 = '' AND p41 = '' AND p42 = '' AND p43 = '' AND p44 = '' AND p45 = '' AND p46 = '' AND p47 = '' AND p48 = '' AND p49 = '' AND p50 = '' AND p51 = '' AND p52 = '' AND p53 = '' AND p54 = '' AND p55 = '' AND p56 = '' AND p57 = '' AND p58 = '' AND p59 = '' AND p60 = '' AND p61 = '' AND p62 = '' AND p63 = ''"));
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf) VALUES (:key, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900') USING TIMESTAMP ?"));
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 >= '[000000]' AND p3 <= '[999999]' "));
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2 AND p3 IN (:p3) "));
    verify(ds, times(1))
        .processBatch(expectedStmts, vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM));
  }

  @Test
  public void insertBatchIfNotExists() {
    ds = mock(TestDataStore.class);
    when(ds.prepare(anyString())).thenCallRealMethod();
    when(ds.processBatch(anyObject(), anyList(), anyObject())).thenCallRealMethod();
    documentDB = new DocumentDB(ds);
    List<String> path = ImmutableList.of("a", "b", "c");
    Map<String, Object> map = documentDB.newBindMap(path, 1L);
    map.put("bool_value", null);
    map.put("dbl_value", 3.0);
    map.put("text_value", null);
    List<Object[]> vars = new ArrayList<>();
    vars.add(map.values().toArray());
    documentDB.insertBatchIfNotExists("keyspace", "table", "key", vars);

    List<PreparedStatement> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf) VALUES (:key, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900') IF NOT EXISTS"));
    expectedStmts.add(
        new TestPreparedStatement(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (:key, :p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12, :p13, :p14, :p15, :p16, :p17, :p18, :p19, :p20, :p21, :p22, :p23, :p24, :p25, :p26, :p27, :p28, :p29, :p30, :p31, :p32, :p33, :p34, :p35, :p36, :p37, :p38, :p39, :p40, :p41, :p42, :p43, :p44, :p45, :p46, :p47, :p48, :p49, :p50, :p51, :p52, :p53, :p54, :p55, :p56, :p57, :p58, :p59, :p60, :p61, :p62, :p63, :leaf, :text_value, :dbl_value, :bool_value) USING TIMESTAMP ?"));
    verify(ds, times(1))
        .processBatch(expectedStmts, vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM));
  }

  @Test
  public void delete() {
    ds = mock(TestDataStore.class);
    when(ds.prepare(anyString())).thenCallRealMethod();
    when(ds.processBatch(anyObject(), anyList(), anyObject())).thenCallRealMethod();
    when(ds.getRecentStatements()).thenCallRealMethod();
    when(ds.getRecentVars()).thenCallRealMethod();
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

    List<PreparedStatement> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 = :p1 AND p2 = :p2"));
    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
    assertThat(ds.getRecentVars().size()).isEqualTo(1);
    assertThat(Arrays.equals(vars.get(0), ds.getRecentVars().get(0))).isTrue();
  }

  @Test
  public void deleteDeadLeaves() {
    ds = mock(TestDataStore.class);
    when(ds.prepare(anyString())).thenCallRealMethod();
    when(ds.processBatch(anyObject(), anyList(), anyObject())).thenCallRealMethod();
    when(ds.getRecentStatements()).thenCallRealMethod();
    when(ds.getRecentVars()).thenCallRealMethod();
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

    documentDB.deleteDeadLeaves("keyspace", "table", "key", deadLeaves);

    List<PreparedStatement> expectedStmts = new ArrayList<>();
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 IN (:p1) "));
    expectedStmts.add(
        new TestPreparedStatement(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = :key AND p0 = :p0 AND p1 >= '[000000]' AND p1 <= '[999999]' "));
    assertThat(ds.getRecentStatements()).isEqualTo(expectedStmts);
    assertThat(ds.getRecentVars().size()).isEqualTo(2);
    Object[] firstVars = new Object[3];
    firstVars[0] = "key";
    firstVars[1] = "a";
    firstVars[2] = "b";

    Object[] secondVars = new Object[2];
    secondVars[0] = "key";
    secondVars[1] = "b";

    // Exclude the timestamp in comparisons here
    assertThat(
            Arrays.equals(
                firstVars,
                Arrays.copyOfRange(ds.getRecentVars().get(0), 1, ds.getRecentVars().get(0).length)))
        .isTrue();
    assertThat(
            Arrays.equals(
                secondVars,
                Arrays.copyOfRange(ds.getRecentVars().get(1), 1, ds.getRecentVars().get(1).length)))
        .isTrue();
  }

  private class TestPreparedStatement implements PreparedStatement {
    private String cql;

    public TestPreparedStatement(String cql) {
      this.cql = cql;
    }

    @Override
    public CompletableFuture<ResultSet> execute(
        DataStore dataStore, Optional<ConsistencyLevel> optional, Object... objects) {
      return null;
    }

    public String getCql() {
      return cql;
    }

    public int hashCode() {
      return this.cql.hashCode();
    }

    public boolean equals(Object other) {
      return other instanceof TestPreparedStatement
          && ((TestPreparedStatement) other).getCql().equals(this.cql);
    }

    public String toString() {
      return this.cql;
    }
  }

  private class TestDataStore implements DataStore {
    private List<PreparedStatement> recentStatements;
    private List<Object[]> recentVars;

    @Override
    public CompletableFuture<ResultSet> query(
        String s, Optional<ConsistencyLevel> optional, Object... objects) {
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    @Override
    public PreparedStatement prepare(String s) {
      return new TestPreparedStatement(s);
    }

    @Override
    public PreparedStatement prepare(String s, Optional<Index> optional) {
      return new TestPreparedStatement(s);
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
    public CompletableFuture<ResultSet> processBatch(
        List<PreparedStatement> statements,
        List<Object[]> vals,
        Optional<ConsistencyLevel> consistencyLevel) {
      System.out.println(statements);
      this.recentStatements = statements;
      this.recentVars = vals;
      return CompletableFuture.completedFuture(ResultSet.empty());
    }

    public List<PreparedStatement> getRecentStatements() {
      return this.recentStatements;
    }

    public List<Object[]> getRecentVars() {
      return this.recentVars;
    }
  }
}
