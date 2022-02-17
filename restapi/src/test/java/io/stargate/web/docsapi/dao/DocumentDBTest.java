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
