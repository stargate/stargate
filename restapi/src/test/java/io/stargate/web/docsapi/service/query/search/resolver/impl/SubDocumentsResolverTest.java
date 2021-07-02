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

package io.stargate.web.docsapi.service.query.search.resolver.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Or;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.ValidatingDataStore;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubDocumentsResolverTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 4;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class GetDocuments {

    @Mock DocsApiConfiguration configuration;

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    @Mock BaseCondition condition;

    @Captor ArgumentCaptor<List<Row>> rowsCaptor;

    QueryExecutor queryExecutor;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      queryExecutor = new QueryExecutor(datastore());
      when(configuration.getSearchPageSize()).thenReturn(100);
      when(configuration.getMaxDepth()).thenReturn(MAX_DEPTH);
      lenient().when(filterExpression.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);
      lenient().when(filterExpression.getCondition()).thenReturn(condition);
      lenient().when(filterExpression2.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);
      lenient().when(filterExpression2.getCondition()).thenReturn(condition);
    }

    @Test
    public void happyPath() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "first"),
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "second")));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              filterExpression, documentId, subDocumentPath, executionContext, true);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .assertValueAt(
              0,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertValueAt(
              1,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

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
                assertThat(nested.description()).isEqualTo("LoadSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathNoSplit() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "first"),
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "second")));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              filterExpression, documentId, subDocumentPath, executionContext, false);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .assertValueAt(
              0,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(2);
                return true;
              })
          .assertComplete();

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
                assertThat(nested.description()).isEqualTo("LoadSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void complexPathWithMoreRows() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Arrays.asList("*", "reviews");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 > ? AND p1 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "p0", "x60", "p1", "reviews", "p2", "[000001]", "p3", "text"),
                      ImmutableMap.of(
                          "key", "1", "p0", "x60", "p1", "reviews", "p2", "[000001]", "p3", "date"),
                      ImmutableMap.of(
                          "key", "1", "p0", "x60", "p1", "reviews", "p2", "[000002]", "p3", "text"),
                      ImmutableMap.of(
                          "key", "1", "p0", "x60", "p1", "reviews", "p2", "[000002]", "p3", "date"),
                      ImmutableMap.of(
                          "key", "1", "p0", "x90", "p1", "reviews", "p2", "[000001]", "p3", "text"),
                      ImmutableMap.of(
                          "key",
                          "1",
                          "p0",
                          "x90",
                          "p1",
                          "reviews",
                          "p2",
                          "[000001]",
                          "p3",
                          "date")));
      DocumentsResolver resolver =
          new SubDocumentsResolver(
              filterExpression, documentId, subDocumentPath, executionContext, true);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .assertValueAt(
              0,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(2);
                return true;
              })
          .assertValueAt(
              1,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(2);
                return true;
              })
          .assertValueAt(
              2,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(2);
                return true;
              })
          .assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(3)).test(rowsCaptor.capture());

      assertThat(rowsCaptor.getAllValues())
          .hasSize(3)
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
                assertThat(rows.get(0).getString("p0")).isEqualTo("x60");
                assertThat(rows.get(0).getString("p2")).isEqualTo("[000002]");
                assertThat(rows.get(0).getString("p3")).isEqualTo("text");
                assertThat(rows.get(0).getString("p0")).isEqualTo("x60");
                assertThat(rows.get(1).getString("p2")).isEqualTo("[000002]");
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
                    .isEqualTo("LoadSubDocuments: sub-path '*.reviews'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(6);
                        });
              });
    }

    @Test
    public void orConditionOneTrue() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "first"),
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "second")));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              true);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .assertValueAt(
              0,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertValueAt(
              1,
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

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
                assertThat(nested.description()).isEqualTo("LoadSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void orConditionNoneTrue() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);
      when(filterExpression.matchesFilterPath(any())).thenReturn(true);
      when(filterExpression2.matchesFilterPath(any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "first"),
                      ImmutableMap.of("key", "1", "p0", "parent", "p1", "second")));

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              true);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().assertValueCount(0).assertComplete();

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
                assertThat(nested.description()).isEqualTo("LoadSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void nothingFound() {
      int pageSize = 1;
      String documentId = "d123456";
      List<String> subDocumentPath = Collections.singletonList("parent");
      Paginator paginator = new Paginator(null, pageSize);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING")
              .withPageSize(configuration.getSearchPageSize())
              .returningNothing();

      DocumentsResolver resolver =
          new SubDocumentsResolver(
              Or.of(filterExpression, filterExpression2),
              documentId,
              subDocumentPath,
              executionContext,
              true);
      Flowable<RawDocument> result =
          resolver.getDocuments(
              queryExecutor, configuration, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().assertValueCount(0).assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(rowsCaptor.capture());

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("LoadSubDocuments: sub-path 'parent'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }
  }
}
