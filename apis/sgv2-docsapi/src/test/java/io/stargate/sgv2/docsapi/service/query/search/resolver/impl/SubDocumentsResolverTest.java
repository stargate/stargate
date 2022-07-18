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

package io.stargate.sgv2.docsapi.service.query.search.resolver.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Or;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.OpenMocksTest;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class SubDocumentsResolverTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Nested
  class GetDocuments implements OpenMocksTest {

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    @Mock BaseCondition condition;

    @Captor ArgumentCaptor<List<RowWrapper>> rowsCaptor;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      lenient().when(filterExpression.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);
      lenient().when(filterExpression.getCondition()).thenReturn(condition);
      lenient().when(filterExpression2.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);
      lenient().when(filterExpression2.getCondition()).thenReturn(condition);
    }

    @Test
    public void happyPath() throws Exception {
      int pageSize = 1;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("parent"),
                  Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      ImmutableList.of(Values.of("1"), Values.of("parent"), Values.of("first")),
                      ImmutableList.of(Values.of("1"), Values.of("parent"), Values.of("second"))));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              filterExpression, documentId, subDocumentPath, executionContext, documentProperties);
      List<RawDocument> result =
          resolver
              .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(1))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(2);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(1)).test(rowsCaptor.capture());

      assertThat(rowsCaptor.getAllValues())
          .hasSize(1)
          .anySatisfy(
              rows ->
                  assertThat(rows)
                      .hasSize(2)
                      .anySatisfy(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("first");
                          })
                      .anySatisfy(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("second");
                          }));

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .startsWith("SearchSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void complexPathWithMoreRows() {
      int pageSize = 1;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Arrays.asList("*", "reviews");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 > ? AND p1 = ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of(""),
                  Values.of("reviews"),
                  Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("x60"),
                          Values.of("reviews"),
                          Values.of("[000001]"),
                          Values.of("text")),
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("x60"),
                          Values.of("reviews"),
                          Values.of("[000001]"),
                          Values.of("date")),
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("x90"),
                          Values.of("reviews"),
                          Values.of("[000001]"),
                          Values.of("text")),
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("x90"),
                          Values.of("reviews"),
                          Values.of("[000001]"),
                          Values.of("date"))));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              filterExpression, documentId, subDocumentPath, executionContext, documentProperties);
      List<RawDocument> result =
          resolver
              .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result).hasSize(2);
      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1", "x60", "reviews");
                assertThat(doc.rows()).hasSize(2);
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1", "x90", "reviews");
                assertThat(doc.rows()).hasSize(2);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(2)).test(rowsCaptor.capture());

      assertThat(rowsCaptor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              rows -> {
                assertThat(rows.get(0).getString("p0")).isEqualTo("x60");
                assertThat(rows.get(0).getString("p2")).isEqualTo("[000001]");
                assertThat(rows.get(0).getString("p3")).isEqualTo("text");
                assertThat(rows.get(0).getString("p0")).isEqualTo("x60");
                assertThat(rows.get(1).getString("p2")).isEqualTo("[000001]");
                assertThat(rows.get(1).getString("p3")).isEqualTo("date");
              })
          .anySatisfy(
              rows -> {
                assertThat(rows.get(0).getString("p0")).isEqualTo("x90");
                assertThat(rows.get(0).getString("p2")).isEqualTo("[000001]");
                assertThat(rows.get(0).getString("p3")).isEqualTo("text");
                assertThat(rows.get(0).getString("p0")).isEqualTo("x90");
                assertThat(rows.get(1).getString("p2")).isEqualTo("[000001]");
                assertThat(rows.get(1).getString("p3")).isEqualTo("date");
              });

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .startsWith("SearchSubDocuments: sub-path '*.reviews'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(4);
                        });
              });
    }

    @Test
    public void orConditionOneTrue() {
      int pageSize = 1;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("*");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 > ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      ImmutableList.of(Values.of("1"), Values.of("parent1"), Values.of("first")),
                      ImmutableList.of(Values.of("1"), Values.of("parent2"), Values.of("second"))));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              documentProperties);
      List<RawDocument> result =
          resolver
              .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result).hasSize(2);
      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1", "parent1");
                assertThat(doc.rows()).hasSize(1);
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.documentKeys()).containsExactly("1", "parent2");
                assertThat(doc.rows()).hasSize(1);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(2)).test(rowsCaptor.capture());

      assertThat(rowsCaptor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("first");
                          }))
          .anySatisfy(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("second");
                          }));

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).startsWith("SearchSubDocuments: sub-path '*'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void orConditionNoneTrue() {
      int pageSize = 1;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("*");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);
      when(filterExpression2.matchesFilterPath(any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 > ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      ImmutableList.of(Values.of("1"), Values.of("parent1"), Values.of("first")),
                      ImmutableList.of(Values.of("1"), Values.of("parent2"), Values.of("second"))));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(2)).test(rowsCaptor.capture());

      assertThat(rowsCaptor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("first");
                          }))
          .anySatisfy(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          row -> {
                            assertThat(row.getString("p1")).isEqualTo("second");
                          }));

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).startsWith("SearchSubDocuments: sub-path '*'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void nothingFound() {
      int pageSize = 1;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("parent"),
                  Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(rowsCaptor.capture());

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .startsWith("SearchSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }
  }
}
