package io.stargate.web.docsapi.dao;

import static io.stargate.db.query.TypedValue.javaValues;
import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.SchemaBuilder.SchemaBuilder__5;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.json.DeadLeaf;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeaf;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DocumentDBTest {

  private static final DocsApiConfiguration config = DocsApiConfiguration.DEFAULT;
  private static final Schema schema = buildSchema();

  private final ExecutionContext context = ExecutionContext.NOOP_CONTEXT;
  private DocumentDB documentDB;
  private TestDataStore ds;

  private static Schema buildSchema() {
    SchemaBuilder__5 schemaBuilder =
        Schema.build()
            .keyspace("keyspace")
            .table("table")
            .column("key", Type.Text, PartitionKey)
            .column("leaf", Type.Text)
            .column("text_value", Type.Text)
            .column("dbl_value", Type.Double)
            .column("bool_value", Type.Boolean);

    for (int i = 0; i < config.getMaxDepth(); i++) {
      schemaBuilder = schemaBuilder.column("p" + i, Type.Text, Clustering);
    }

    return schemaBuilder.build();
  }

  @BeforeEach
  public void setup() throws UnauthorizedException {
    ds = new TestDataStore(schema);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));
    documentDB =
        new DocumentDB(ds, AuthenticationSubject.of("foo", "bar"), authorizationService, config);
  }

  @Test
  public void getInsertStatement() {
    Object[] values =
        new Object[DocsApiConstants.ALL_COLUMNS_NAMES.apply(config.getMaxDepth()).length];
    int idx = 0;
    values[idx++] = "key";
    for (int i = 0; i < config.getMaxDepth(); i++) {
      values[idx++] = "value" + i;
    }
    values[idx++] = "leaf";
    values[idx++] = "text";
    values[idx++] = 1.2d; // dbl_value
    values[idx++] = true; // bool_value
    assertThat(idx).isEqualTo(values.length);

    BoundQuery query = documentDB.getInsertStatement("keyspace", "table", 1, values);
    assertThat(query.queryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
  }

  @Test
  public void getPrefixDeleteStatement() {
    BoundQuery query =
        documentDB.getPrefixDeleteStatement(
            "keyspace", "table", "k", 1, Arrays.asList("a", "b", "c"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ?");
    assertThat(javaValues(query.values())).isEqualTo(asList(1L, "k", "a", "b", "c"));
  }

  @Test
  public void getSubpathArrayDeleteStatement() {
    BoundQuery query =
        documentDB.getSubpathArrayDeleteStatement(
            "keyspace", "table", "k", 1, Arrays.asList("a", "b", "c", "d"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 >= ? AND p4 <= ?");
    assertThat(javaValues(query.values()))
        .isEqualTo(asList(1L, "k", "a", "b", "c", "d", "[000000]", "[999999]"));
  }

  @Test
  public void getPathKeysDeleteStatement() {
    BoundQuery query =
        documentDB.getPathKeysDeleteStatement(
            "keyspace",
            "table",
            "k",
            1,
            Arrays.asList("a", "b", "c", "d", "e"),
            Arrays.asList("a", "few", "keys"));
    assertThat(query.queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND p5 IN ?");
    assertThat(javaValues(query.values()))
        .isEqualTo(asList(1L, "k", "a", "b", "c", "d", "e", Arrays.asList("a", "few", "keys")));
  }

  @Test
  public void getExactPathDeleteStatement() {
    BoundQuery query =
        documentDB.getExactPathDeleteStatement(
            "keyspace", "table", "key", 1, Arrays.asList("a", "b", "c", "d", "e"));
    StringBuilder expected =
        new StringBuilder("DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ?");
    for (int i = 0; i < 64; i++) {
      expected.append(" AND p").append(i).append(" = ?");
    }
    assertThat(query.queryString()).isEqualTo(expected.toString());

    List<Object> expectedValues = new ArrayList<>();
    expectedValues.add(1L);
    expectedValues.add("key");
    expectedValues.addAll(Arrays.asList("a", "b", "c", "d", "e"));
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
  public void deleteThenInsertBatch() throws UnauthorizedException {
    ds = new TestDataStore(schema);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));
    documentDB =
        new DocumentDB(ds, AuthenticationSubject.of("foo", "bar"), authorizationService, config);
    List<String> path = Arrays.asList("a", "b", "c");
    Map<String, Object> map = DocsApiUtils.newBindMap(path, config.getMaxDepth());
    map.put("key", "key");
    map.put("leaf", "c");
    map.put("bool_value", true);
    map.put("dbl_value", null);
    map.put("text_value", null);
    Object[] values = map.values().toArray();
    documentDB.deleteThenInsertBatch(
        "keyspace", "table", "key", singletonList(values), path, 1L, context);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(2);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ?");
    assertThat(javaValues(generatedQueries.get(0).values())).isEqualTo(makeValues(0L, "key", path));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
    assertThat(javaValues(generatedQueries.get(1).values())).isEqualTo(makeValues(values, 1L));
  }

  @Test
  public void deletePatchedPathsThenInsertBatch() throws UnauthorizedException {
    ds = new TestDataStore(schema);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));
    documentDB =
        new DocumentDB(ds, AuthenticationSubject.of("foo", "bar"), authorizationService, config);
    List<String> path = Arrays.asList("a", "b", "c");
    List<String> patchedKeys = Arrays.asList("eric");
    Map<String, Object> map = DocsApiUtils.newBindMap(path, config.getMaxDepth());
    map.put("key", "key");
    map.put("leaf", "c");
    map.put("bool_value", null);
    map.put("dbl_value", 3.0);
    map.put("text_value", null);
    Object[] values = map.values().toArray();
    documentDB.deletePatchedPathsThenInsertBatch(
        "keyspace", "table", "key", singletonList(values), path, patchedKeys, 1L, context);

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(4);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "INSERT INTO \"keyspace\".\"table\" (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");
    assertThat(javaValues(generatedQueries.get(0).values())).isEqualTo(makeValues(values, 1L));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND p5 = ? AND p6 = ? AND p7 = ? AND p8 = ? AND p9 = ? AND p10 = ? AND p11 = ? AND p12 = ? AND p13 = ? AND p14 = ? AND p15 = ? AND p16 = ? AND p17 = ? AND p18 = ? AND p19 = ? AND p20 = ? AND p21 = ? AND p22 = ? AND p23 = ? AND p24 = ? AND p25 = ? AND p26 = ? AND p27 = ? AND p28 = ? AND p29 = ? AND p30 = ? AND p31 = ? AND p32 = ? AND p33 = ? AND p34 = ? AND p35 = ? AND p36 = ? AND p37 = ? AND p38 = ? AND p39 = ? AND p40 = ? AND p41 = ? AND p42 = ? AND p43 = ? AND p44 = ? AND p45 = ? AND p46 = ? AND p47 = ? AND p48 = ? AND p49 = ? AND p50 = ? AND p51 = ? AND p52 = ? AND p53 = ? AND p54 = ? AND p55 = ? AND p56 = ? AND p57 = ? AND p58 = ? AND p59 = ? AND p60 = ? AND p61 = ? AND p62 = ? AND p63 = ?");
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
  public void deleteDeadLeaves() throws UnauthorizedException {
    ds = new TestDataStore(schema);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));
    documentDB =
        new DocumentDB(ds, AuthenticationSubject.of("foo", "bar"), authorizationService, config);

    Map<String, Set<DeadLeaf>> deadLeaves = new LinkedHashMap<>();
    deadLeaves.put("$.a.b", new HashSet<>());
    deadLeaves.get("$.a.b").add(ImmutableDeadLeaf.builder().name("").build());

    deadLeaves.put("$.b", new HashSet<>());
    deadLeaves.get("$.b").add(DeadLeaf.ARRAYLEAF);

    documentDB.deleteDeadLeaves("keyspace", "table", "key", 1L, deadLeaves, context).join();

    List<BoundQuery> generatedQueries = ds.getRecentStatements();
    assertThat(generatedQueries).hasSize(2);

    assertThat(generatedQueries.get(0).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 IN ?");
    assertThat(javaValues(generatedQueries.get(0).values()))
        .isEqualTo(asList(1L, "key", "a", "b", singletonList("")));

    assertThat(generatedQueries.get(1).queryString())
        .isEqualTo(
            "DELETE FROM \"keyspace\".\"table\" USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 >= ? AND p1 <= ?");
    assertThat(javaValues(generatedQueries.get(1).values()))
        .isEqualTo(makeValues(1L, "key", "b", "[000000]", "[999999]"));
  }

  private static class TestDataStore implements DataStore {

    private final List<BoundQuery> recentQueries = new ArrayList<>();
    private final Schema schema;

    public TestDataStore(Schema schema) {
      this.schema = schema;
    }

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
    public boolean supportsLoggedBatches() {
      return true;
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
      return schema;
    }

    @Override
    public boolean isInSchemaAgreement() {
      return true;
    }

    @Override
    public void waitForSchemaAgreement() {}

    @Override
    public boolean supportsSecondaryIndex() {
      return true;
    }

    @Override
    public boolean supportsSAI() {
      return true;
    }

    public List<BoundQuery> getRecentStatements() {
      List<BoundQuery> recent = new ArrayList<>(this.recentQueries);
      this.recentQueries.clear();
      return recent;
    }
  }
}
