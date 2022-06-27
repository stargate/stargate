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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * @author Dmitri Bourlatchkov
 * @author Ivan Senic
 */
@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class QueryExecutorTest extends AbstractValidatingStargateBridgeTest {

  public static final Function<List<QueryOuterClass.Value>, ByteBuffer>
      FIRST_COLUMN_COMPARABLE_KEY = row -> ByteBuffer.wrap(row.get(0).getString().getBytes());
  private final ExecutionContext context = ExecutionContext.NOOP_CONTEXT;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Inject DocumentProperties documentProperties;

  List<QueryOuterClass.ColumnSpec> columnSpec;
  QueryOuterClass.Query allDocsQuery;

  @BeforeEach
  public void initColumnSpec() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    // using only sub-set of columns
    columnSpec =
        schemaProvider
            .allColumnSpecStream()
            .filter(
                c -> {
                  String column = c.getName();

                  return Objects.equals(column, tableProps.keyColumnName())
                      || Objects.equals(column, tableProps.doubleValueColumnName())
                      || column.startsWith(tableProps.pathColumnPrefix());
                })
            .toList();
    // init all docs query
    allDocsQuery =
        new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();
  }

  List<QueryOuterClass.Value> row(String id, String p0, Double value) {
    return row(id, p0, "", "", value);
  }

  List<QueryOuterClass.Value> row(String id, String p0, String p1, String p2, Double value) {
    return ImmutableList.of(
        Values.of(id),
        Values.of(p0),
        Values.of(p1),
        Values.of(p2),
        Values.of(""),
        Values.of(value));
  }

  void withFiveTestDocs(
      QueryOuterClass.Query query, int pageSize, QueryOuterClass.ResumeMode resumeMode) {
    withQuery(query.getCql())
        .enriched()
        .withResumeMode(resumeMode)
        .withPageSize(pageSize)
        .withColumnSpec(columnSpec)
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

  void withFiveTestDocIds(QueryOuterClass.Query query, int pageSize) {
    withQuery(query.getCql())
        .enriched()
        .withPageSize(pageSize)
        .withColumnSpec(columnSpec)
        .returning(
            ImmutableList.of(
                row("1", "x", 1.0d),
                row("2", "x", 3.0d),
                row("3", "x", 1.0d),
                row("4", "y", 2.0d),
                row("5", "x", 3.0d)));
  }

  @Nested
  class QueryDocs {

    @Test
    public void fullScan() {
      List<List<QueryOuterClass.Value>> rows =
          ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d));

      withQuery(allDocsQuery.getCql()).enriched().withColumnSpec(columnSpec).returning(rows);

      List<RawDocument> result =
          queryExecutor
              .queryDocs(allDocsQuery, 100, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result)
          .hasSize(2)
          .anySatisfy(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1");
                assertThat(doc.rows()).hasSize(2);
              })
          .anySatisfy(
              doc -> {
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.documentKeys()).containsExactly("2");
                assertThat(doc.rows()).hasSize(1);
              });
    }

    @ParameterizedTest
    @CsvSource({"1", "3", "5", "100"})
    public void fullScanPaged(int pageSize) {
      QueryOuterClass.Query query =
          new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

      withFiveTestDocs(query, pageSize, QueryOuterClass.ResumeMode.NEXT_PARTITION);

      List<RawDocument> result =
          queryExecutor
              .queryDocs(query, pageSize, false, null, true, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(5))
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");
      assertThat(result.subList(0, 4)).allSatisfy(doc -> assertThat(doc.hasPagingState()).isTrue());

      // test paging from each doc returned
      for (int i = 1; i < 5; i++) {
        ByteBuffer ps1 = result.get(i - 1).makePagingState();
        List<RawDocument> subResult =
            queryExecutor
                .queryDocs(query, pageSize, false, ps1, true, context)
                .subscribe()
                .withSubscriber(AssertSubscriber.create(5))
                .awaitCompletion()
                .assertCompleted()
                .getItems();

        List<String> expected =
            IntStream.range(i + 1, 5).mapToObj(String::valueOf).collect(Collectors.toList());
        assertThat(subResult).hasSize(5 - i).extracting(RawDocument::id).containsAll(expected);
      }
    }

    @ParameterizedTest
    @CsvSource({"1", "100", "5000"})
    public void fullScanDeep(int pageSize) {
      int N = 100_000;
      // generate 100_000 data elements
      ImmutableList.Builder<List<QueryOuterClass.Value>> rows = ImmutableList.builder();
      for (int i = 0; i < N; i++) {
        rows.add(row("" + i, "a", 11.0d));
      }

      QueryOuterClass.Query query =
          new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

      withQuery(query.getCql())
          .withPageSize(pageSize)
          .enriched()
          .withColumnSpec(columnSpec)
          .returning(rows.build());

      // Testing potential stack overflow in reactive pipelines
      queryExecutor
          .queryDocs(query, pageSize, false, null, false, context)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(N))
          .awaitItems(N)
          .awaitCompletion()
          .assertCompleted();
    }

    @ParameterizedTest
    @CsvSource({"4", "10", "20", "50", "100", "500", "1000", "5000"})
    public void fullScanPartial(int pageSize) {
      // generate 10 pages of data
      ImmutableList.Builder<List<QueryOuterClass.Value>> rows = ImmutableList.builder();
      for (int i = 0; i < 10 * pageSize; i++) {
        rows.add(row("" + i, "a", 11.0d));
      }

      QueryOuterClass.Query query =
          new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(query.getCql())
              .withPageSize(pageSize)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(columnSpec)
              .returning(rows.build());

      AssertSubscriber<RawDocument> assertSubscriber =
          queryExecutor
              .queryDocs(query, pageSize, false, null, true, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1));

      RawDocument lastItem = assertSubscriber.awaitItems(1).getLastItem();
      assertThat(lastItem.id()).isEqualTo("0");
      queryAssert.assertExecuteCount().isEqualTo(1); // first page fetched

      lastItem = assertSubscriber.awaitNextItems(pageSize - 3).getLastItem();
      assertThat(lastItem.id()).isEqualTo(String.valueOf(pageSize - 3));
      queryAssert.assertExecuteCount().isEqualTo(1); // still on page 1

      // request one more, total requested here == pageSize - 1
      // One more row is requested from upstream to detect doc boundaries, which ends page 1
      // and causes page 2 to be executed
      lastItem = assertSubscriber.awaitNextItems(1).getLastItem();
      assertThat(lastItem.id()).isEqualTo(String.valueOf(pageSize - 2));
      queryAssert.assertExecuteCount().isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    public void fullScanFinalPagingState(int pageSize) {
      // Note: the test result set has 8 rows.
      // If page size is a divisor of the result set size the test DataStore will return a non-null
      // paging state in the page that contains the last row to emulate C* behaviour, which cannot
      // say for sure that there are no more rows available in this case. Therefore, in that case
      // the last doc will have a non-null paging state, but using it will yield no other docs.
      boolean shouldHavePagingState = (8 % pageSize) == 0;

      QueryOuterClass.Query query =
          new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

      withFiveTestDocs(query, pageSize, QueryOuterClass.ResumeMode.NEXT_PARTITION);

      List<RawDocument> result =
          queryExecutor
              .queryDocs(query, pageSize, false, null, true, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(5))
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("1", "2", "3", "4", "5");

      RawDocument lastDocument = result.get(4);
      assertThat(lastDocument.hasPagingState()).isEqualTo(shouldHavePagingState);
      ByteBuffer pagingState = lastDocument.makePagingState();
      assertThat(pagingState).matches(buf -> (buf == null) == !shouldHavePagingState);

      if (pagingState != null) {
        queryExecutor
            .queryDocs(query, pageSize, false, pagingState, true, context)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(1))
            .awaitCompletion()
            .assertHasNotReceivedAnyItem();
      }
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    public void populate(int pageSize) {
      QueryOuterClass.Query selectAllQuery =
          new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

      withFiveTestDocIds(selectAllQuery, pageSize);

      BuiltCondition condition =
          BuiltCondition.of(
              documentProperties.tableProperties().keyColumnName(), Predicate.EQ, Term.marker());
      QueryOuterClass.Query selectDocQuery =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(condition)
              .build();

      withQuery(selectDocQuery.getCql(), Values.of("2"))
          .withPageSize(pageSize)
          .enriched()
          .withColumnSpec(columnSpec)
          .returning(ImmutableList.of(row("2", "x", 3.0d), row("2", "y", 1.0d)));

      withQuery(selectDocQuery.getCql(), Values.of("5"))
          .withPageSize(pageSize)
          .enriched()
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("5", "x", 3.0d),
                  row("5", "z", 2.0d),
                  row("5", "y", 1.0d),
                  row("6", "x", 1.0d))); // the last row should be ignored

      List<RawDocument> result =
          queryExecutor
              .queryDocs(selectAllQuery, pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(5))
              .awaitCompletion()
              .getItems();

      assertThat(result).hasSize(5);

      RawDocument doc2NotPopulated = result.get(1);
      assertThat(doc2NotPopulated.rows()).hasSize(1);
      assertThat(doc2NotPopulated.id()).isEqualTo("2");

      QueryOuterClass.Values.Builder doc2Values =
          QueryOuterClass.Values.newBuilder().addValues(Values.of("2"));
      QueryOuterClass.Query doc2Query =
          QueryOuterClass.Query.newBuilder(selectDocQuery).setValues(doc2Values).build();
      RawDocument doc2 =
          doc2NotPopulated
              .populateFrom(
                  queryExecutor.queryDocs(doc2Query, pageSize, false, null, false, context))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(doc2.id()).isEqualTo("2");
      assertThat(doc2.rows()).hasSize(2);
      assertThat(doc2.hasPagingState()).isTrue();
      assertThat(doc2.makePagingState()).isEqualTo(doc2NotPopulated.makePagingState());

      RawDocument doc5NotPopulated = result.get(4);
      assertThat(doc5NotPopulated.id()).isEqualTo("5");
      assertThat(doc5NotPopulated.rows()).hasSize(1);

      QueryOuterClass.Values.Builder doc5Values =
          QueryOuterClass.Values.newBuilder().addValues(Values.of("5"));
      QueryOuterClass.Query doc5Query =
          QueryOuterClass.Query.newBuilder(selectDocQuery).setValues(doc5Values).build();
      RawDocument doc5 =
          doc5NotPopulated
              .populateFrom(
                  queryExecutor.queryDocs(doc5Query, pageSize, false, null, false, context))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(doc5.id()).isEqualTo("5");
      assertThat(doc5.rows()).hasSize(3);
      assertThat(doc5.hasPagingState()).isEqualTo(doc5NotPopulated.hasPagingState());
      assertThat(doc5.makePagingState()).isEqualTo(doc5NotPopulated.makePagingState());
    }

    @Test
    public void subDocuments() {
      BuiltCondition keyCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().keyColumnName(), Predicate.EQ, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(keyCondition)
              .build();

      QueryOuterClass.Value[] values = {Values.of("b")};
      withQuery(query.getCql(), values)
          .enriched()
          .withPageSize(3)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("b", "x", "2", "f1", 1.0d),
                  row("b", "x", "2", "f2", 2.0d),
                  row("b", "x", "2", "f3", 3.0d),
                  row("b", "x", "2", "f4", 4.0d),
                  row("b", "y", "2", "f1", 5.0d),
                  row("b", "y", "3", "f1", 6.0d),
                  row("b", "y", "3", "f2", 7.0d)));

      QueryOuterClass.Values.Builder queryValues =
          QueryOuterClass.Values.newBuilder().addAllValues(Arrays.asList(values));
      QueryOuterClass.Query finalQuery =
          QueryOuterClass.Query.newBuilder(query).setValues(queryValues).build();
      List<RawDocument> result =
          queryExecutor
              .queryDocs(3, finalQuery, 3, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(3))
              .awaitItems(3)
              .awaitCompletion()
              .getItems();

      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.rows())
                    .extracting(r -> r.getDouble("dbl_value"))
                    .containsExactly(1.0d, 2.0d, 3.0d, 4.0d);
                assertThat(doc.documentKeys()).containsExactly("b", "x", "2");
              });

      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.rows())
                    .extracting(r -> r.getDouble("dbl_value"))
                    .containsExactly(5.0d);
                assertThat(doc.documentKeys()).containsExactly("b", "y", "2");
              });

      assertThat(result.get(2))
          .satisfies(
              doc -> {
                assertThat(doc.rows())
                    .extracting(r -> r.getDouble("dbl_value"))
                    .containsExactly(6.0d, 7.0d);
                assertThat(doc.documentKeys()).containsExactly("b", "y", "3");
              });
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    void subDocumentsPaged(int pageSize) {
      BuiltCondition keyCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().keyColumnName(), Predicate.EQ, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(keyCondition)
              .build();

      QueryOuterClass.Value[] values = {Values.of("a")};
      withQuery(query.getCql(), values)
          .enriched()
          .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
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

      QueryOuterClass.Values.Builder queryValues =
          QueryOuterClass.Values.newBuilder().addAllValues(Arrays.asList(values));
      QueryOuterClass.Query finalQuery =
          QueryOuterClass.Query.newBuilder(query).setValues(queryValues).build();
      List<RawDocument> result =
          queryExecutor
              .queryDocs(3, finalQuery, pageSize, false, null, true, context)
              .select()
              .first(2)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .getItems();

      assertThat(result)
          .hasSize(2)
          .anySatisfy(doc -> assertThat(doc.documentKeys()).containsExactly("a", "x", "2"))
          .anySatisfy(doc -> assertThat(doc.documentKeys()).containsExactly("a", "y", "2"));

      ByteBuffer ps2 = result.get(1).makePagingState();
      List<RawDocument> pagedResult =
          queryExecutor
              .queryDocs(3, finalQuery, pageSize, false, ps2, true, context)
              .select()
              .first(2)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .getItems();

      assertThat(pagedResult)
          .hasSize(2)
          .anySatisfy(
              doc -> {
                assertThat(doc.documentKeys()).containsExactly("a", "y", "3");
                assertThat(doc.rows())
                    .extracting(r -> r.getDouble("dbl_value"))
                    .containsExactly(8.0d, 9.0d);
                assertThat(doc.hasPagingState()).isTrue();
              })
          .anySatisfy(
              doc -> {
                assertThat(doc.documentKeys()).containsExactly("a", "y", "4");
                assertThat(doc.rows())
                    .extracting(r -> r.getDouble("dbl_value"))
                    .containsExactly(10.0d);
              });

      // Depending on how pages align with the result set, the last doc may or may not have a paging
      // state (see testFullScanFinalPagingState(...) for details). In any case, though, there
      // should be no more documents in the pipeline.
      ByteBuffer lastPagingState = pagedResult.get(1).makePagingState();
      if (lastPagingState != null) {
        queryExecutor
            .queryDocs(3, finalQuery, pageSize, false, lastPagingState, true, context)
            .select()
            .first(2)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(1))
            .awaitCompletion()
            .assertHasNotReceivedAnyItem();
      }
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    void mergeByDocKey(int pageSize) {
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      withQuery(query.getCql(), valueX)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", 1.0d),
                  row("a", "x2", 2.0d),
                  row("c", "x2", 2.0d),
                  row("c", "x3", 2.0d)));
      QueryOuterClass.Value valueY = Values.of("y");
      withQuery(query.getCql(), valueY)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x2", 2.0d),
                  row("b", "y1", 1.0d),
                  row("b", "y2", 1.0d),
                  row("d", "z1", 1.0d),
                  row("d", "z2", 2.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(4))
              .awaitItems(4)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "b", "c", "d");
      assertThat(result.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
      assertThat(result.get(1).rows()).extracting(r -> r.getString("p0")).contains("y2");
      assertThat(result.get(2).rows()).extracting(r -> r.getString("p0")).contains("x2", "x3");
      assertThat(result.get(3).rows()).extracting(r -> r.getString("p0")).contains("z1", "z2");
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    void mergeSubDocuments(int pageSize) {
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      withQuery(query.getCql(), valueX)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", "y1", "", 1.0d),
                  row("a", "x1", "y1", "z1", 2.0d),
                  row("a", "x2", "y1", "z1", 3.0d),
                  row("b", "x1", "y1", "z1", 4.0d),
                  row("c", "x1", "y1", "", 5.0d)));

      QueryOuterClass.Value valueY = Values.of("y");
      withQuery(query.getCql(), valueY)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", "y1", "z1", 2.0d),
                  row("a", "x2", "y1", "z2", 6.0d),
                  row("b", "x1", "y1", "", 7.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(2, ImmutableList.of(q1, q2), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(4))
              .awaitItems(4)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "a", "b", "c");

      assertThat(result.get(0).documentKeys()).containsExactly("a", "x1");
      assertThat(result.get(0).rows())
          .extracting(r -> r.getDouble("dbl_value"))
          .containsExactly(1.0d, 2.0d);
      assertThat(result.get(1).documentKeys()).containsExactly("a", "x2");
      assertThat(result.get(1).rows())
          .extracting(r -> r.getDouble("dbl_value"))
          .containsExactly(3.0d, 6.0d);
      assertThat(result.get(2).documentKeys()).containsExactly("b", "x1");
      assertThat(result.get(2).rows())
          .extracting(r -> r.getDouble("dbl_value"))
          .containsExactly(7.0d, 4.0d);
      assertThat(result.get(3).documentKeys()).containsExactly("c", "x1");
      assertThat(result.get(3).rows())
          .extracting(r -> r.getDouble("dbl_value"))
          .containsExactly(5.0d);
    }

    // not supported as we don't know the selected clustering columns
    // @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    public void mergeWithPartialPath(int pageSize) {
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      withQuery(query.getCql(), valueX)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", "y1", "", 1.0d),
                  row("a", "x1", "y1", "z1", 2.0d),
                  row("a", "x2", "y1", "z1", 3.0d),
                  row("b", "x1", "y1", "z1", 4.0d),
                  row("c", "x1", "y1", "", 5.0d)));

      QueryOuterClass.Value valueY = Values.of("y");
      withQuery(query.getCql(), valueY)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", "y1", "z1", 2.0d),
                  row("a", "x2", "y1", "z2", 6.0d),
                  row("b", "x1", "y1", "", 7.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(2, ImmutableList.of(q1, q2), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(4))
              .awaitItems(4)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "a", "b", "c");

      // Note: clustering key columns that were not explicitly selected are not used to distinguish
      // document property rows.
      assertThat(result.get(0).documentKeys()).containsExactly("a", "x1");
      assertThat(result.get(0).rows()).hasSize(1);
      assertThat(result.get(1).documentKeys()).containsExactly("a", "x2");
      assertThat(result.get(1).rows()).hasSize(1);
      assertThat(result.get(2).documentKeys()).containsExactly("b", "x1");
      assertThat(result.get(2).rows()).hasSize(1);
      assertThat(result.get(3).documentKeys()).containsExactly("c", "x1");
      assertThat(result.get(3).rows()).hasSize(1);
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    public void testMergeByDocKeyPaged(int pageSize) {
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      withQuery(query.getCql(), valueX)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", 1.0d),
                  row("a", "x2", 2.0d),
                  row("b", "x1", 2.0d),
                  row("b", "x3", 2.0d),
                  row("c", "x2", 2.0d),
                  row("d", "x2", 2.0d)));

      QueryOuterClass.Value valueY = Values.of("y");
      withQuery(query.getCql(), valueY)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x2", 2.0d),
                  row("a", "x3", 1.0d),
                  row("b", "x2", 1.0d),
                  row("b", "x4", 1.0d),
                  row("d", "x1", 2.0d)));

      QueryOuterClass.Value valueZ = Values.of("z");
      withQuery(query.getCql(), valueZ)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(ImmutableList.of(row("c", "x1", 1.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();
      QueryOuterClass.Query q3 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueZ))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2, q3), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(4))
              .awaitItems(4)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "b", "c", "d");
      assertThat(result.get(0).rows())
          .extracting(r -> r.getString("p0"))
          .contains("x1", "x2", "x3");
      assertThat(result.get(0).makePagingState()).isNotNull();

      ByteBuffer pageState1 = result.get(0).makePagingState();
      List<RawDocument> resultPaged1 =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2, q3), pageSize, false, pageState1, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(3))
              .awaitItems(3)
              .awaitCompletion()
              .getItems();

      assertThat(resultPaged1).extracting(RawDocument::id).containsExactly("b", "c", "d");
      assertThat(resultPaged1.get(0).rows())
          .extracting(r -> r.getString("p0"))
          .contains("x1", "x2", "x3", "x4");
      assertThat(resultPaged1.get(0).makePagingState()).isNotNull();

      ByteBuffer pageState2 = resultPaged1.get(0).makePagingState();
      List<RawDocument> resultPaged2 =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2, q3), pageSize, false, pageState2, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .getItems();

      assertThat(resultPaged2).extracting(RawDocument::id).containsExactly("c", "d");
      assertThat(resultPaged2.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1");
      assertThat(resultPaged2.get(0).makePagingState()).isNotNull();

      ByteBuffer pageState3 = resultPaged2.get(0).makePagingState();
      List<RawDocument> resultPaged3 =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2, q3), pageSize, false, pageState3, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitItems(1)
              .awaitCompletion()
              .getItems();

      boolean shouldHasPagingState = 1 == pageSize;
      assertThat(resultPaged3)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("d");
                assertThat(doc.rows()).extracting(r -> r.getString("p0")).contains("x1");
                assertThat(doc.hasPagingState()).isEqualTo(shouldHasPagingState);
              });

      if (shouldHasPagingState) {
        ByteBuffer pageState4 = resultPaged3.get(0).makePagingState();
        queryExecutor
            .queryDocs(ImmutableList.of(q1, q2, q3), pageSize, false, pageState4, false, context)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(1))
            .awaitCompletion()
            .assertHasNotReceivedAnyItem();
      }
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"})
    public void mergeResultPaginationWithExcludedRows(int pageSize) {
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      withQuery(query.getCql(), valueX)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row("a", "x1", 1.0d),
                  row("a", "x2", 2.0d), // this row is duplicated in the second result set
                  row("b", "x1", 3.0d)));

      QueryOuterClass.Value valueY = Values.of("y");
      withQuery(query.getCql(), valueY)
          .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
          .enriched()
          .withPageSize(pageSize)
          .withColumnSpec(columnSpec)
          .returning(
              ImmutableList.of(
                  row(
                      "a", "x2",
                      2.0d), // note: this is the last row for "a", plus it is a duplicate
                  row("b", "x2", 3.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "b");
      assertThat(result.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
      assertThat(result.get(0).makePagingState()).isNotNull();

      ByteBuffer pageState1 = result.get(0).makePagingState();
      List<RawDocument> pagedResult =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2), pageSize, false, pageState1, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitItems(1)
              .awaitCompletion()
              .getItems();

      assertThat(pagedResult).extracting(RawDocument::id).containsExactly("b");
      assertThat(pagedResult.get(0).rows()).extracting(r -> r.getString("p0")).contains("x1", "x2");
    }

    @Test
    void testExhaustedQueryReExecution() {
      int pageSize = 1000;
      BuiltCondition pathCondition =
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(0), Predicate.GT, Term.marker());

      QueryOuterClass.Query query =
          new QueryBuilder()
              .select()
              .star()
              .from(schemaProvider.getTable().getName())
              .where(pathCondition)
              .build();

      QueryOuterClass.Value valueX = Values.of("x");
      ValidatingStargateBridge.QueryAssert queryAssert1 =
          withQuery(query.getCql(), valueX)
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .enriched()
              .withPageSize(pageSize)
              .withColumnSpec(columnSpec)
              .returning(ImmutableList.of(row("a", "x1", 1.0d), row("a", "x2", 2.0d)));

      QueryOuterClass.Value valueY = Values.of("y");
      ValidatingStargateBridge.QueryAssert queryAssert2 =
          withQuery(query.getCql(), valueY)
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .enriched()
              .withPageSize(pageSize)
              .withColumnSpec(columnSpec)
              .returning(ImmutableList.of(row("b", "x2", 1.0d), row("b", "x4", 2.0d)));

      QueryOuterClass.Query q1 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueX))
              .build();
      QueryOuterClass.Query q2 =
          QueryOuterClass.Query.newBuilder(query)
              .setValues(QueryOuterClass.Values.newBuilder().addValues(valueY))
              .build();

      List<RawDocument> result =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2), pageSize, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .getItems();

      assertThat(result).extracting(RawDocument::id).containsExactly("a", "b");
      queryAssert1.assertExecuteCount().isEqualTo(1);
      queryAssert2.assertExecuteCount().isEqualTo(1);

      ByteBuffer pageState1 = result.get(0).makePagingState();
      List<RawDocument> pagedResult =
          queryExecutor
              .queryDocs(ImmutableList.of(q1, q2), pageSize, false, pageState1, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitItems(1)
              .awaitCompletion()
              .getItems();

      assertThat(pagedResult).extracting(RawDocument::id).containsExactly("b");

      // Note: we exhausted the first query, so there was no need to re-execute it
      queryAssert1.assertExecuteCount().isEqualTo(1);
      queryAssert2.assertExecuteCount().isEqualTo(2);
    }

    @Test
    public void identityDepthValidation() {
      Throwable failureUnderMin =
          queryExecutor
              .queryDocs(-123, allDocsQuery, 1, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitFailure()
              .getFailure();

      assertThat(failureUnderMin)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid document identity depth: -123");

      Throwable failureOverMax =
          queryExecutor
              .queryDocs(6, allDocsQuery, 1, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitFailure()
              .getFailure();

      assertThat(failureOverMax)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid document identity depth: 6");
    }
  }

  @Nested
  @TestProfile(WithDifferentConsistency.Profile.class)
  class WithDifferentConsistency {

    public static class Profile extends MaxDepth4TestProfile {

      @Override
      public Map<String, String> getConfigOverrides() {
        return ImmutableMap.<String, String>builder()
            .putAll(super.getConfigOverrides())
            .put("stargate.queries.consistency.reads", "ONE")
            .build();
      }
    }

    @Test
    public void fullScan() {
      List<List<QueryOuterClass.Value>> rows =
          ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d));

      withQuery(allDocsQuery.getCql())
          .enriched()
          .withColumnSpec(columnSpec)
          .withConsistency(QueryOuterClass.Consistency.ONE)
          .returning(rows);

      List<RawDocument> result =
          queryExecutor
              .queryDocs(allDocsQuery, 100, false, null, false, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result)
          .hasSize(2)
          .anySatisfy(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1");
                assertThat(doc.rows()).hasSize(2);
              })
          .anySatisfy(
              doc -> {
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.documentKeys()).containsExactly("2");
                assertThat(doc.rows()).hasSize(1);
              });
    }
  }
}
