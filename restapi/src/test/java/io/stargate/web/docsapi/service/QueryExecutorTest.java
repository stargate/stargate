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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.Flowable;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.service.QueryExecutor.RawDocument;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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

  private QueryExecutor executor;
  private AbstractBound<?> allDocsQuery;

  @Override
  protected Schema schema() {
    return schema;
  }

  @BeforeEach
  public void setup() {
    executor = new QueryExecutor(datastore(), table);
    allDocsQuery = datastore().queryBuilder().select().star().from(table).build().bind();
  }

  private Map<String, Object> row(String id, String p0, Double value) {
    return ImmutableMap.of("key", id, "p0", p0, "test_value", value);
  }

  private <T> List<T> get(Flowable<T> flowable) {
    return StreamSupport.stream(flowable.blockingIterable().spliterator(), false)
        .collect(Collectors.toList());
  }

  @Test
  void testFullScan() {
    withQuery(table, "SELECT * FROM %s")
        .returning(ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d)));

    List<RawDocument> r1 = get(executor.queryDocs(allDocsQuery, 2, null));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2");
  }

  @Test
  void testFullScanLimited() {
    withFiveTestDocs(3);

    assertThat(get(executor.queryDocs(allDocsQuery, 1, null)))
        .extracting(RawDocument::id)
        .containsExactly("1");

    assertThat(get(executor.queryDocs(allDocsQuery, 2, null)))
        .extracting(RawDocument::id)
        .containsExactly("1", "2");

    assertThat(get(executor.queryDocs(allDocsQuery, 4, null)))
        .extracting(RawDocument::id)
        .containsExactly("1", "2", "3", "4");

    assertThat(get(executor.queryDocs(allDocsQuery, 100, null)))
        .extracting(RawDocument::id)
        .containsExactly("1", "2", "3", "4", "5");
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

  @ParameterizedTest
  @CsvSource({"1", "3", "5", "100"})
  void testFullScanPaged(int pageSize) {
    withFiveTestDocs(pageSize);

    List<RawDocument> r1 = get(executor.queryDocs(allDocsQuery, 4, null));
    assertThat(r1).extracting(RawDocument::id).containsExactly("1", "2", "3", "4");

    ByteBuffer ps1 = r1.get(0).makePagingState();
    List<RawDocument> r2 = get(executor.queryDocs(allDocsQuery, 2, ps1));
    assertThat(r2).extracting(RawDocument::id).containsExactly("2", "3");

    ByteBuffer ps2 = r1.get(1).makePagingState();
    List<RawDocument> r3 = get(executor.queryDocs(allDocsQuery, 3, ps2));
    assertThat(r3).extracting(RawDocument::id).containsExactly("3", "4", "5");

    ByteBuffer ps4 = r1.get(3).makePagingState();
    List<RawDocument> r4 = get(executor.queryDocs(allDocsQuery, 100, ps4));
    assertThat(r4).extracting(RawDocument::id).containsExactly("5");
  }

  @Test
  void testResultSetPagination() {
    withFiveTestDocs(3);

    List<ResultSet> r1 =
        get(
            executor.execute(
                datastore().queryBuilder().select().star().from(table).build().bind(), null));

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
}
