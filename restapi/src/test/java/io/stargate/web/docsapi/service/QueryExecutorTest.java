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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList.Builder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.ValidatingDataStore.QueryAssert;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class QueryExecutorTest extends AbstractDataStoreTest {

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

  private static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("test_docs").addTables(table).build();

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
    executor = new QueryExecutor(datastore());
    allDocsQuery = datastore().queryBuilder().select().star().from(table).build().bind();
    allRowsForDocQuery =
        datastore().queryBuilder().select().star().from(table).where("key", Predicate.EQ).build();
  }

  private Map<String, Object> row(String id, String p0, Double value) {
    return ImmutableMap.of("key", id, "p0", p0, "test_value", value);
  }

  private Map<String, Object> row(String id, String p0, String p1, Double value) {
    return ImmutableMap.of("key", id, "p0", p0, "p1", p1, "test_value", value);
  }

  @Test
  void testFullScan() {
    withQuery(table, "SELECT * FROM %s")
        .returning(ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d)));

    List<RawDocument> r1 = executor.queryDocs(allDocsQuery, 100, null, context).test().values();
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2");
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
                row("5", "x", 3.0d)));
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
  void testFullScanPaged(int pageSize) {
    withFiveTestDocs(pageSize);

    List<RawDocument> r1 =
        executor.queryDocs(allDocsQuery, pageSize, null, context).test().values();
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");
    assertThat(r1.get(0).hasPagingState()).isTrue();
    assertThat(r1.get(1).hasPagingState()).isTrue();
    assertThat(r1.get(2).hasPagingState()).isTrue();
    assertThat(r1.get(3).hasPagingState()).isTrue();

    ByteBuffer ps1 = r1.get(0).makePagingState();
    List<RawDocument> r2 = executor.queryDocs(allDocsQuery, pageSize, ps1, context).test().values();
    assertThat(r2).extracting(RawDocument::id).containsExactly("2", "3", "4", "5");

    ByteBuffer ps2 = r1.get(1).makePagingState();
    List<RawDocument> r3 = executor.queryDocs(allDocsQuery, pageSize, ps2, context).test().values();
    assertThat(r3).extracting(RawDocument::id).containsExactly("3", "4", "5");

    ByteBuffer ps4 = r1.get(3).makePagingState();
    List<RawDocument> r4 = executor.queryDocs(allDocsQuery, pageSize, ps4, context).test().values();
    assertThat(r4).extracting(RawDocument::id).containsExactly("5");
  }

  @ParameterizedTest
  @CsvSource({"1", "100", "5000"})
  void testFullScanDeep(int pageSize) {
    int N = 100_000;
    Builder<Map<String, Object>> rows = ImmutableList.builder();
    for (int i = 0; i < N; i++) { // generate 10 pages of data
      rows.add(row("" + i, "a", 11.0d)); // one row per document
    }

    withQuery(table, "SELECT * FROM %s").withPageSize(pageSize).returning(rows.build());

    // Testing potential stack overflow in Rx pipelines
    assertThat(executor.queryDocs(allDocsQuery, pageSize, null, context).test().values())
        .hasSize(N);
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

    Flowable<RawDocument> flowable = executor.queryDocs(allDocsQuery, pageSize, null, context);
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
  @CsvSource({"1", "3", "5", "100"})
  void testFullScanFinalPagingState(int pageSize) {
    withFiveTestDocs(pageSize);

    List<RawDocument> r1 =
        executor.queryDocs(allDocsQuery, pageSize, null, context).test().values();
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");
    assertThat(r1.get(4).makePagingState()).isNull();
    assertThat(r1.get(4).hasPagingState()).isFalse();
  }

  @ParameterizedTest
  @CsvSource({"1", "3", "5", "100"})
  void testPopulate(int pageSize) {
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

    List<RawDocument> r1 =
        executor.queryDocs(allDocsQuery, pageSize, null, context).test().values();
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");

    RawDocument doc2a = r1.get(1);
    assertThat(doc2a.rows()).hasSize(1);
    assertThat(doc2a.id()).isEqualTo("2");

    RawDocument doc2b =
        doc2a
            .populateFrom(executor.queryDocs(allRowsForDocQuery.bind("2"), pageSize, null, context))
            .blockingGet();
    assertThat(doc2b.id()).isEqualTo("2");
    assertThat(doc2b.rows()).hasSize(2);
    assertThat(doc2b.hasPagingState()).isTrue();
    assertThat(doc2b.makePagingState()).isEqualTo(doc2a.makePagingState());

    RawDocument doc5a = r1.get(4);
    assertThat(doc5a.id()).isEqualTo("5");
    assertThat(doc5a.rows()).hasSize(1);
    assertThat(doc5a.makePagingState()).isNull();
    assertThat(doc5a.hasPagingState()).isFalse();

    RawDocument doc5b =
        doc5a
            .populateFrom(executor.queryDocs(allRowsForDocQuery.bind("5"), pageSize, null, context))
            .blockingGet();
    assertThat(doc5b.id()).isEqualTo("5");
    assertThat(doc5b.rows()).hasSize(3);
    assertThat(doc5b.makePagingState()).isNull();
    assertThat(doc5b.hasPagingState()).isFalse();
  }

  @Test
  void testResultSetPagination() {
    withFiveTestDocs(3);

    List<ResultSet> r1 =
        executor
            .execute(datastore().queryBuilder().select().star().from(table).build().bind(), 3, null)
            .test()
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
  void testSubDocuments() {
    withQuery(table, "SELECT * FROM %s WHERE key = ? AND p0 > ?", "a", "x")
        .withPageSize(3)
        .returning(
            ImmutableList.of(
                row("b", "x", "2", 1.0d),
                row("b", "x", "2", 2.0d),
                row("b", "x", "2", 3.0d),
                row("b", "x", "2", 4.0d),
                row("b", "y", "2", 5.0d),
                row("b", "y", "3", 6.0d),
                row("b", "y", "3", 7.0d)));

    List<RawDocument> docs =
        executor
            .queryDocs(
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
                null,
                context)
            .test()
            .values();

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
  @CsvSource({"1", "3", "5", "100"})
  void testSubDocumentsPaged(int pageSize) {
    withQuery(table, "SELECT * FROM %s WHERE key = ? AND p0 > ?", "a", "x")
        .withPageSize(pageSize)
        .returning(
            ImmutableList.of(
                row("a", "x", "2", 1.0d),
                row("a", "x", "2", 2.0d),
                row("a", "x", "2", 3.0d),
                row("a", "x", "2", 4.0d),
                row("a", "y", "2", 5.0d),
                row("a", "y", "2", 6.0d),
                row("a", "y", "3", 8.0d),
                row("a", "y", "3", 9.0d),
                row("a", "y", "4", 10.0d)));

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
        executor.queryDocs(3, query, pageSize, null, context).take(2).test().values();

    assertThat(docs).hasSize(2);
    assertThat(docs.get(0).key()).containsExactly("a", "x", "2");

    assertThat(docs.get(1).key()).containsExactly("a", "y", "2");

    assertThat(docs.get(1).hasPagingState()).isTrue();
    ByteBuffer ps2 = docs.get(1).makePagingState();
    assertThat(ps2).isNotNull();

    docs = executor.queryDocs(3, query, pageSize, ps2, context).test().values();

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
    assertThat(docs.get(1).hasPagingState()).isFalse();
    assertThat(docs.get(1).makePagingState()).isNull();
  }
}
