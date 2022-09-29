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
package io.stargate.web.docsapi.service;

import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Regular;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList.Builder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.ValidatingDataStore.QueryAssert;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.stargate.exceptions.OverloadedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueryExecutorTest extends AbstractDataStoreTest {
  @Mock private DocsApiConfiguration config;

  protected static final Table table =
      ImmutableTable.builder()
          .keyspace("test_docs")
          .name("collection1")
          .addColumns(
              ImmutableColumn.builder().name("key").type(Type.Text).kind(PartitionKey).build())
          .addColumns(ImmutableColumn.builder().name("p0").type(Type.Text).kind(Clustering).build())
          .addColumns(ImmutableColumn.builder().name("p1").type(Type.Text).kind(Clustering).build())
          .addColumns(ImmutableColumn.builder().name("p2").type(Type.Text).kind(Clustering).build())
          .addColumns(ImmutableColumn.builder().name("p3").type(Type.Text).kind(Clustering).build())
          .addColumns(
              ImmutableColumn.builder().name("test_value").type(Type.Double).kind(Regular).build())
          .build();

  protected static final Table table2 =
      ImmutableTable.builder()
          .keyspace("test_docs")
          .name("collection2")
          .addColumns(
              ImmutableColumn.builder().name("key").type(Type.Text).kind(PartitionKey).build())
          .build();

  private static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("test_docs").addTables(table, table2).build();

  private static final Schema schema = ImmutableSchema.builder().addKeyspaces(keyspace).build();

  private final ExecutionContext context = ExecutionContext.NOOP_CONTEXT;
  private QueryExecutor executor;
  private AbstractBound<?> allDocsQuery;
  private BuiltQuery<?> allRowsForDocQuery;

  @Override
  protected Schema schema() {
    return schema;
  }

  @BeforeEach
  public void setup() {
    executor = new QueryExecutor(datastore(), config);
    allDocsQuery = datastore().queryBuilder().select().star().from(table).build().bind();
    allRowsForDocQuery =
        datastore().queryBuilder().select().star().from(table).where("key", Predicate.EQ).build();
  }

  private Map<String, Object> row(String id, String p0, Double value) {
    return row(id, p0, "", "", value);
  }

  private Map<String, Object> row(String id, String p0, String p1, String p2, Double value) {
    return ImmutableMap.<String, Object>builder()
        .put("key", id)
        .put("p0", p0)
        .put("p1", p1)
        .put("p2", p2)
        .put("p3", "")
        .put("test_value", value)
        .build();
  }

  @Test
  void testFullScan() throws Exception {
    withQuery(table, "SELECT * FROM %s")
        .returning(ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d)));

    List<RawDocument> r1 = values(executor.queryDocs(allDocsQuery, 100, false, null, context));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2");
  }

  @Test
  void testFullScanWithExecutionException() throws Exception {
    OverloadedException error = new OverloadedException("Testing purposes");
    withQuery(table, "SELECT * FROM %s").withExecutionException(error);

    executor.queryDocs(allDocsQuery, 100, false, null, context).test().await().assertError(error);

    resetExpectations();
  }

  private void withFiveTestDocs(int pageSize) {
    withQuery(table, "SELECT * FROM %s")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("1", "x", 1.0d),
                row("1", "y", 2.0d),
                row("2", "x", 3.0d),
                row("3", "x", 1.0d),
                row("4", "y", 2.0d),
                row("4", "x", 3.0d),
                row("5", "x", 3.0d),
                row("5", "y", 3.0d)));
  }

  private void withFiveTestDocIds(int pageSize) {
    withQuery(table, "SELECT * FROM %s")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("1", "x", 1.0d),
                row("2", "x", 3.0d),
                row("3", "x", 1.0d),
                row("4", "y", 2.0d),
                row("5", "x", 3.0d)));
  }

  @ParameterizedTest
  @CsvSource({"1", "3", "5", "100"})
  void testFullScanPaged(int pageSize) throws Exception {
    withFiveTestDocs(pageSize);

    List<RawDocument> r1 = values(executor.queryDocs(allDocsQuery, pageSize, false, null, context));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");
    assertThat(r1.get(0).hasPagingState()).isTrue();
    assertThat(r1.get(1).hasPagingState()).isTrue();
    assertThat(r1.get(2).hasPagingState()).isTrue();
    assertThat(r1.get(3).hasPagingState()).isTrue();

    ByteBuffer ps1 = r1.get(0).makePagingState();
    List<RawDocument> r2 = values(executor.queryDocs(allDocsQuery, pageSize, false, ps1, context));
    assertThat(r2).extracting(RawDocument::id).containsExactly("2", "3", "4", "5");

    ByteBuffer ps2 = r1.get(1).makePagingState();
    List<RawDocument> r3 = values(executor.queryDocs(allDocsQuery, pageSize, false, ps2, context));
    assertThat(r3).extracting(RawDocument::id).containsExactly("3", "4", "5");

    ByteBuffer ps4 = r1.get(3).makePagingState();
    List<RawDocument> r4 = values(executor.queryDocs(allDocsQuery, pageSize, false, ps4, context));
    assertThat(r4).extracting(RawDocument::id).containsExactly("5");
  }

  @ParameterizedTest
  @CsvSource({"1", "100", "5000"})
  void testFullScanDeep(int pageSize) throws Exception {
    int N = 100_000;
    Builder<Map<String, Object>> rows = ImmutableList.builder();
    for (int i = 0; i < N; i++) { // generate 10 pages of data
      rows.add(row("" + i, "a", 11.0d)); // one row per document
    }

    withQuery(table, "SELECT * FROM %s").withPageSize(pageSize).returning(rows.build());

    // Testing potential stack overflow in Rx pipelines
    assertThat(values(executor.queryDocs(allDocsQuery, pageSize, false, null, context))).hasSize(N);
  }

  @ParameterizedTest
  @CsvSource({"4", "10", "20", "50", "100", "500", "1000", "5000"})
  void testPartialFullScan(int pageSize) {
    Builder<Map<String, Object>> rows = ImmutableList.builder();
    for (int i = 0; i <= 10 * pageSize; i++) { // generate 10 pages of data
      rows.add(row("" + i, "a", 11.0d)); // one row per document
    }

    QueryAssert queryAssert =
        withQuery(table, "SELECT * FROM %s").withPageSize(pageSize).returning(rows.build());

    Flowable<RawDocument> flowable =
        executor.queryDocs(allDocsQuery, pageSize, false, null, context);
    TestSubscriber<RawDocument> test = flowable.test(1);

    test.awaitCount(1);
    test.assertValueAt(0, d -> d.id().equals("0"));
    queryAssert.assertExecuteCount().isEqualTo(1); // first page fetched

    test.request(pageSize - 3); // total requested == pageSize - 2
    test.awaitCount(pageSize - 2);
    test.assertValueAt(pageSize - 3, d -> d.id().equals("" + (pageSize - 3)));
    queryAssert.assertExecuteCount().isEqualTo(1); // still on page 1

    test.request(1);
    // Total requested here == pageSize - 1
    // One more row is requested from upstream to detect doc boundaries, which ends page 1
    // and causes page 2 to be executed
    test.awaitCount(pageSize - 1);
    test.assertValueAt(pageSize - 2, d -> d.id().equals("" + (pageSize - 2)));
    queryAssert.assertExecuteCount().isEqualTo(2);
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testFullScanFinalPagingState(int pageSize) throws Exception {
    // Note: the test result set has 8 rows.
    // If page size is a divisor of the result set size the test DataStore will return a non-null
    // paging state in the page that contains the last row to emulate C* behaviour, which cannot
    // say for sure that there are no more rows available in this case. Therefore, in that case
    // the last doc will have a non-null paging state, but using it will yield no other docs.
    boolean shouldHavePagingState = (8 % pageSize) == 0;
    withFiveTestDocs(pageSize);

    List<RawDocument> r1 = values(executor.queryDocs(allDocsQuery, pageSize, false, null, context));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");
    assertThat(r1.get(4).hasPagingState()).isEqualTo(shouldHavePagingState);
    ByteBuffer pagingState = r1.get(4).makePagingState();
    assertThat(pagingState).matches(buf -> (buf == null) == !shouldHavePagingState);

    if (pagingState != null) {
      List<RawDocument> r2 =
          values(executor.queryDocs(allDocsQuery, pageSize, false, pagingState, context));
      assertThat(r2).isEmpty();
    }
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testPopulate(int pageSize) throws Exception {
    withFiveTestDocIds(pageSize);
    withQuery(table, "SELECT * FROM %s WHERE key = ?", "2")
        .withPageSize(pageSize)
        .returning(ImmutableList.of(row("2", "x", 3.0d), row("2", "y", 1.0d)));
    withQuery(table, "SELECT * FROM %s WHERE key = ?", "5")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("5", "x", 3.0d),
                row("5", "z", 2.0d),
                row("5", "y", 1.0d),
                row("6", "x", 1.0d))); // the last row should be ignored

    List<RawDocument> r1 = values(executor.queryDocs(allDocsQuery, pageSize, false, null, context));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");

    RawDocument doc2a = r1.get(1);
    assertThat(doc2a.rows()).hasSize(1);
    assertThat(doc2a.id()).isEqualTo("2");

    RawDocument doc2b =
        doc2a
            .populateFrom(
                executor.queryDocs(allRowsForDocQuery.bind("2"), pageSize, false, null, context))
            .blockingGet();
    assertThat(doc2b.id()).isEqualTo("2");
    assertThat(doc2b.rows()).hasSize(2);
    assertThat(doc2b.hasPagingState()).isTrue();
    assertThat(doc2b.makePagingState()).isEqualTo(doc2a.makePagingState());

    RawDocument doc5a = r1.get(4);
    assertThat(doc5a.id()).isEqualTo("5");
    assertThat(doc5a.rows()).hasSize(1);

    RawDocument doc5b =
        doc5a
            .populateFrom(
                executor.queryDocs(allRowsForDocQuery.bind("5"), pageSize, false, null, context))
            .blockingGet();
    assertThat(doc5b.id()).isEqualTo("5");
    assertThat(doc5b.rows()).hasSize(3);
    assertThat(doc5b.hasPagingState()).isEqualTo(doc5a.hasPagingState());
    assertThat(doc5b.makePagingState()).isEqualTo(doc5a.makePagingState());
  }

  @Test
  void testResultSetPagination() throws Exception {
    withFiveTestDocs(3);

    List<ResultSet> r1 =
        executor
            .execute(
                datastore().queryBuilder().select().star().from(table).build().bind(),
                3,
                false,
                null)
            .test()
            .await()
            .values();

    assertThat(r1.get(0).currentPageRows())
        .extracting(r -> r.getString("key"))
        .containsExactly("1", "1", "2");
    assertThat(r1.get(0).getPagingState()).isNotNull();
    assertThat(r1.get(1).currentPageRows())
        .extracting(r -> r.getString("key"))
        .containsExactly("3", "4", "4");
    assertThat(r1.get(1).getPagingState()).isNotNull();
    assertThat(r1.get(2).currentPageRows())
        .extracting(r -> r.getString("key"))
        .containsExactly("5", "5");
    assertThat(r1.get(2).getPagingState()).isNull();
    assertThat(r1).hasSize(3);
  }

  @Test
  void testSubDocuments() throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE key = ? AND p0 > ?", "a", "x")
        .withPageSize(3)
        .returning(
            ImmutableList.of(
                row("b", "x", "2", "f1", 1.0d),
                row("b", "x", "2", "f2", 2.0d),
                row("b", "x", "2", "f3", 3.0d),
                row("b", "x", "2", "f4", 4.0d),
                row("b", "y", "2", "f1", 5.0d),
                row("b", "y", "3", "f1", 6.0d),
                row("b", "y", "3", "f2", 7.0d)));

    List<RawDocument> docs =
        values(
            executor.queryDocs(
                3,
                datastore()
                    .queryBuilder()
                    .select()
                    .star()
                    .from(table)
                    .where("key", Predicate.EQ, "a")
                    .where("p0", Predicate.GT, "x")
                    .build()
                    .bind(),
                3,
                false,
                null,
                context));

    assertThat(docs.get(0).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(1.0d, 2.0d, 3.0d, 4.0d);
    assertThat(docs.get(0).key()).containsExactly("b", "x", "2");

    assertThat(docs.get(1).rows()).extracting(r -> r.getDouble("test_value")).containsExactly(5.0d);
    assertThat(docs.get(1).key()).containsExactly("b", "y", "2");

    assertThat(docs.get(2).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(6.0d, 7.0d);
    assertThat(docs.get(2).key()).containsExactly("b", "y", "3");
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testSubDocumentsPaged(int pageSize) throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE key = ? AND p0 > ?", "a", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x", "2", "p1", 1.0d),
                row("a", "x", "2", "p2", 2.0d),
                row("a", "x", "2", "p3", 3.0d),
                row("a", "x", "2", "p4", 4.0d),
                row("a", "y", "2", "p1", 5.0d),
                row("a", "y", "2", "p2", 6.0d),
                row("a", "y", "3", "p1", 8.0d),
                row("a", "y", "3", "p2", 9.0d),
                row("a", "y", "4", "p1", 10.0d)));

    AbstractBound<?> query =
        datastore()
            .queryBuilder()
            .select()
            .star()
            .from(table)
            .where("key", Predicate.EQ, "a")
            .where("p0", Predicate.GT, "x")
            .build()
            .bind();

    List<RawDocument> docs =
        values(executor.queryDocs(3, query, pageSize, false, null, context).take(2));

    assertThat(docs).hasSize(2);
    assertThat(docs.get(0).key()).containsExactly("a", "x", "2");

    assertThat(docs.get(1).key()).containsExactly("a", "y", "2");

    assertThat(docs.get(1).hasPagingState()).isTrue();
    ByteBuffer ps2 = docs.get(1).makePagingState();
    assertThat(ps2).isNotNull();

    docs = values(executor.queryDocs(3, query, pageSize, false, ps2, context));

    assertThat(docs).hasSize(2);
    assertThat(docs.get(0).key()).containsExactly("a", "y", "3");
    assertThat(docs.get(0).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(8.0d, 9.0d);
    assertThat(docs.get(1).key()).containsExactly("a", "y", "4");
    assertThat(docs.get(1).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(10.0d);

    assertThat(docs.get(0).hasPagingState()).isTrue();
    assertThat(docs.get(0).makePagingState()).isNotNull();

    ByteBuffer lastPagingState = docs.get(1).makePagingState();
    assertThat(docs.get(1).hasPagingState()).isEqualTo(lastPagingState != null);

    // Depending on how pages align with the result set, the last doc may or may not have a paging
    // state (see testFullScanFinalPagingState(...) for details). In any case, though, there
    // should be no more documents in the pipeline.
    if (lastPagingState != null) {
      docs = values(executor.queryDocs(3, query, pageSize, false, lastPagingState, context));
      assertThat(docs).isEmpty();
    }
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testMergeByDocKey(int pageSize) throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", 1.0d),
                row("a", "x2", 2.0d),
                row("c", "x2", 2.0d),
                row("c", "x3", 2.0d)));
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "y")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x2", 2.0d),
                row("b", "y1", 1.0d),
                row("b", "y2", 1.0d),
                row("d", "z1", 1.0d),
                row("d", "z2", 2.0d)));

    BuiltQuery<?> query =
        datastore().queryBuilder().select().star().from(table).where("p0", Predicate.GT).build();

    AbstractBound<?> q1 = query.bind("x");
    AbstractBound<?> q2 = query.bind("y");

    List<RawDocument> docs =
        values(executor.queryDocs(ImmutableList.of(q1, q2), pageSize, false, null, context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "b", "c", "d");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
    assertThat(docs.get(1).rows()).extracting(r -> r.getString("p0")).contains("y2");
    assertThat(docs.get(2).rows()).extracting(r -> r.getString("p0")).contains("x2", "x3");
    assertThat(docs.get(3).rows()).extracting(r -> r.getString("p0")).contains("z1", "z2");
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testMergeSubDocuments(int pageSize) throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", "y1", "", 1.0d),
                row("a", "x1", "y1", "z1", 2.0d),
                row("a", "x2", "y1", "z1", 3.0d),
                row("b", "x1", "y1", "z1", 4.0d),
                row("c", "x1", "y1", "", 5.0d)));
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "y")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", "y1", "z1", 2.0d),
                row("a", "x2", "y1", "z2", 6.0d),
                row("b", "x1", "y1", "", 7.0d)));

    BuiltQuery<?> query =
        datastore().queryBuilder().select().star().from(table).where("p0", Predicate.GT).build();

    AbstractBound<?> q1 = query.bind("x");
    AbstractBound<?> q2 = query.bind("y");

    List<RawDocument> docs =
        values(executor.queryDocs(2, ImmutableList.of(q1, q2), pageSize, false, null, context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "a", "b", "c");

    assertThat(docs.get(0).key()).containsExactly("a", "x1");
    assertThat(docs.get(0).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(1.0d, 2.0d);
    assertThat(docs.get(1).key()).containsExactly("a", "x2");
    assertThat(docs.get(1).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(3.0d, 6.0d);
    assertThat(docs.get(2).key()).containsExactly("b", "x1");
    assertThat(docs.get(2).rows())
        .extracting(r -> r.getDouble("test_value"))
        .containsExactly(7.0d, 4.0d);
    assertThat(docs.get(3).key()).containsExactly("c", "x1");
    assertThat(docs.get(3).rows()).extracting(r -> r.getDouble("test_value")).containsExactly(5.0d);
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testMergeWithPartialPath(int pageSize) throws Exception {
    String cql = "SELECT key, p0, test_value FROM %s WHERE p0 > ?";
    withQuery(table, cql, "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", "y1", "", 1.0d),
                row("a", "x1", "y1", "z1", 2.0d),
                row("a", "x2", "y1", "z1", 3.0d),
                row("b", "x1", "y1", "z1", 4.0d),
                row("c", "x1", "y1", "", 5.0d)));
    withQuery(table, cql, "y")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", "y1", "z1", 2.0d),
                row("a", "x2", "y1", "z2", 6.0d),
                row("b", "x1", "y1", "", 7.0d)));

    BuiltQuery<?> query =
        datastore()
            .queryBuilder()
            .select()
            .column("key")
            .column("p0")
            .column("test_value")
            .from(table)
            .where("p0", Predicate.GT)
            .build();

    AbstractBound<?> q1 = query.bind("x");
    AbstractBound<?> q2 = query.bind("y");

    List<RawDocument> docs =
        values(executor.queryDocs(2, ImmutableList.of(q1, q2), pageSize, false, null, context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "a", "b", "c");

    // Note: clustering key columns that were not explicitly selected are not used to distinguish
    // document property rows.
    assertThat(docs.get(0).key()).containsExactly("a", "x1");
    assertThat(docs.get(0).rows()).hasSize(1);
    assertThat(docs.get(1).key()).containsExactly("a", "x2");
    assertThat(docs.get(1).rows()).hasSize(1);
    assertThat(docs.get(2).key()).containsExactly("b", "x1");
    assertThat(docs.get(2).rows()).hasSize(1);
    assertThat(docs.get(3).key()).containsExactly("c", "x1");
    assertThat(docs.get(3).rows()).hasSize(1);
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testMergeByDocKeyPaged(int pageSize) throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", 1.0d),
                row("a", "x2", 2.0d),
                row("b", "x1", 2.0d),
                row("b", "x3", 2.0d),
                row("c", "x2", 2.0d),
                row("d", "x2", 2.0d)));
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "y")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x2", 2.0d),
                row("a", "x3", 1.0d),
                row("b", "x2", 1.0d),
                row("b", "x4", 1.0d),
                row("d", "x1", 2.0d)));
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "z")
        .withPageSize(pageSize)
        .returning(ImmutableList.of(row("c", "x1", 1.0d)));

    BuiltQuery<?> query =
        datastore().queryBuilder().select().star().from(table).where("p0", Predicate.GT).build();

    List<BoundQuery> queries =
        ImmutableList.<BoundQuery>builder()
            .add(query.bind("x"))
            .add(query.bind("y"))
            .add(query.bind("z"))
            .build();

    List<RawDocument> docs = values(executor.queryDocs(queries, pageSize, false, null, context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "b", "c", "d");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2", "x3");
    assertThat(docs.get(0).makePagingState()).isNotNull();

    docs =
        values(
            executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("b", "c", "d");
    assertThat(docs.get(0).rows())
        .extracting(r -> r.getString("p0"))
        .contains("x1", "x2", "x3", "x4");
    assertThat(docs.get(0).makePagingState()).isNotNull();

    docs =
        values(
            executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("c", "d");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1");
    assertThat(docs.get(0).makePagingState()).isNotNull();

    docs =
        values(
            executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("d");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1");

    // See comment in testFullScanFinalPagingState and note that both queries had 1 row in their
    // result sets during the last execution.
    boolean shouldHasPagingState = 1 == pageSize;
    if (!shouldHasPagingState) {
      assertThat(docs.get(0).makePagingState()).isNull();
    } else {
      docs =
          values(
              executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context));
      assertThat(docs).isEmpty();
    }
  }

  @ParameterizedTest
  @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
  void testMergeResultPaginationWithExcludedRows(int pageSize) throws Exception {
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x1", 1.0d),
                row("a", "x2", 2.0d), // this row is duplicated in the second result set
                row("b", "x1", 3.0d)));
    withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "y")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x2", 2.0d), // note: this is the last row for "a", plus it is a duplicate
                row("b", "x2", 3.0d)));

    BuiltQuery<?> query =
        datastore().queryBuilder().select().star().from(table).where("p0", Predicate.GT).build();

    List<BoundQuery> queries =
        ImmutableList.<BoundQuery>builder().add(query.bind("x")).add(query.bind("y")).build();

    List<RawDocument> docs = values(executor.queryDocs(queries, pageSize, false, null, context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "b");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
    assertThat(docs.get(0).makePagingState()).isNotNull();

    docs =
        values(
            executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context));

    assertThat(docs).extracting(RawDocument::id).containsExactly("b");
    assertThat(docs.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
  }

  @Test
  void testExhaustedQueryReExecution() throws Exception {
    int pageSize = 1000;
    QueryAssert q1 =
        withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "x")
            .withPageSize(pageSize)
            .returning(ImmutableList.of(row("a", "x1", 1.0d), row("a", "x2", 2.0d)));
    QueryAssert q2 =
        withQuery(table, "SELECT * FROM %s WHERE p0 > ?", "y")
            .withPageSize(pageSize)
            .returning(ImmutableList.of(row("b", "x2", 1.0d), row("b", "x4", 2.0d)));

    BuiltQuery<?> query =
        datastore().queryBuilder().select().star().from(table).where("p0", Predicate.GT).build();

    List<BoundQuery> queries =
        ImmutableList.<BoundQuery>builder().add(query.bind("x")).add(query.bind("y")).build();

    Flowable<RawDocument> result = executor.queryDocs(queries, pageSize, false, null, context);

    List<RawDocument> docs = values(result);
    assertThat(docs).extracting(RawDocument::id).containsExactly("a", "b");
    q1.assertExecuteCount().isEqualTo(1);
    q2.assertExecuteCount().isEqualTo(1);

    Flowable<RawDocument> result2 =
        executor.queryDocs(queries, pageSize, false, docs.get(0).makePagingState(), context);

    docs = values(result2);
    assertThat(docs).extracting(RawDocument::id).containsExactly("b");

    // Note: we exhausted the first query, so there was no need to re-execute it
    q1.assertExecuteCount().isEqualTo(1);
    q2.assertExecuteCount().isEqualTo(2);
  }

  @Test
  void testIdentityDepthValidation() {
    assertThatThrownBy(() -> executor.queryDocs(-123, allDocsQuery, 1, false, null, context))
        .hasMessageContaining("Invalid document identity depth: -123");
    assertThatThrownBy(() -> executor.queryDocs(123, allDocsQuery, 1, false, null, context))
        .hasMessageContaining("Invalid document identity depth: 123");
  }

  @Test
  void testIdentityColumnValidation() {
    assertThatThrownBy(
            () ->
                executor.queryDocs(
                    1,
                    datastore().queryBuilder().select().column("p0").from(table).build().bind(),
                    1,
                    false,
                    null,
                    context))
        .hasMessageContaining("Required identity column is not selected")
        .hasMessageContaining("key");

    assertThatThrownBy(
            () ->
                executor.queryDocs(
                    3,
                    datastore()
                        .queryBuilder()
                        .select()
                        .column("key")
                        .column("p1")
                        .from(table)
                        .build()
                        .bind(),
                    1,
                    false,
                    null,
                    context))
        .hasMessageContaining("Required identity column is not selected")
        .hasMessageContaining("p0");
  }

  @Test
  void testSelectionSetValidation() {
    assertThatThrownBy(
            () ->
                executor.queryDocs(
                    1,
                    ImmutableList.of(
                        datastore()
                            .queryBuilder()
                            .select()
                            .column("key")
                            .from(table)
                            .build()
                            .bind(),
                        datastore()
                            .queryBuilder()
                            .select()
                            .column("p0")
                            .from(table)
                            .build()
                            .bind()),
                    1,
                    false,
                    null,
                    context))
        .hasMessageContaining("Incompatible sets of columns are selected by the provided queries")
        .hasMessageContaining("key")
        .hasMessageContaining("p0");
  }

  @Test
  void testTableValidation() {
    assertThatThrownBy(() -> executor.queryDocs(1, ImmutableList.of(), 1, false, null, context))
        .hasMessageContaining("No tables are referenced by the provided queries");

    assertThatThrownBy(
            () ->
                executor.queryDocs(
                    1,
                    ImmutableList.of(
                        datastore().queryBuilder().select().star().from(table).build().bind(),
                        datastore().queryBuilder().select().star().from(table2).build().bind()),
                    1,
                    false,
                    null,
                    context))
        .hasMessageContaining("Too many tables are referenced by the provided queries")
        .hasMessageContaining(table.name())
        .hasMessageContaining(table2.name());
  }
}
