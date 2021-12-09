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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Or;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.datastore.AbstractDataStoreTest;
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
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableGenericCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableNumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrExpressionDocumentsResolverTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 4;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Mock DocsApiConfiguration configuration;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class GetDocuments {

    QueryExecutor queryExecutor;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      queryExecutor = new QueryExecutor(datastore(), configuration);
      lenient().when(configuration.getMaxStoragePageSize()).thenReturn(100);
      lenient().when(configuration.getApproximateStoragePageSize(anyInt())).thenCallRealMethod();
      when(configuration.getMaxDepth()).thenReturn(MAX_DEPTH);
    }

    @Test
    public void twoPersistenceConditions() throws Exception {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 = ImmutableNumberCondition.of(LtFilterOperation.of(), 1);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  1d)
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field LT 1)'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .allSatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void twoPersistenceConditionsOneQueryEmpty() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 = ImmutableNumberCondition.of(LtFilterOperation.of(), 1);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(2)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  1d)
              .withPageSize(2)
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field LT 1)'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void twoPersistenceConditionsNothingReturned() throws Exception {
      int pageSize = 2;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 = ImmutableNumberCondition.of(LtFilterOperation.of(), 1);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returningNothing();

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  1d)
              .withPageSize(pageSize + 1)
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field LT 1)'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .allSatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void persistenceAndInMemoryConditionPersistenceTrue() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      BaseCondition condition2 =
          ImmutableGenericCondition.of(InFilterOperation.of(), Collections.singletonList(1), false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(2)
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          "1",
                          "leaf",
                          "field",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "text_value",
                          "query-value")));

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(2)
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key", "1", "leaf", "field", "p0", "field", "p1", "", "dbl_value", 2d)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field EQ query-value | field IN [1])'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .allSatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void persistenceAndInMemoryConditionMemoryTrue() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 =
          ImmutableGenericCondition.of(InFilterOperation.of(), Collections.singletonList(1), false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(2)
              .returningNothing();

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(2)
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key", "1", "leaf", "field", "p0", "field", "p1", "", "dbl_value", 1d)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field IN [1])'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void persistenceAndInMemoryConditionMemoryFalse() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 =
          ImmutableGenericCondition.of(InFilterOperation.of(), Collections.singletonList(1), false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(2)
              .returningNothing();

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(2)
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key", "1", "leaf", "field", "p0", "field", "p1", "", "dbl_value", 2d)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field IN [1])'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void persistenceAndInMemoryConditionNothingReturned() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("path", "field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 =
          ImmutableGenericCondition.of(InFilterOperation.of(), Collections.singletonList(1), false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value > ? ALLOW FILTERING",
                  "path",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(2)
              .returningNothing();

      ValidatingDataStore.QueryAssert query2Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? ALLOW FILTERING",
                  "path",
                  "field",
                  "field",
                  "")
              .withPageSize(2)
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);
      query2Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo(
                        "MERGING OR: expression '(path.field GT query-value | path.field IN [1])'");
                assertThat(nested.queries())
                    .hasSize(2)
                    .allSatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void persistenceAndEvaluateOnMissing() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 = ImmutableStringCondition.of(NeFilterOperation.of(), "not-me");
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s")
              .withPageSize(configuration.getApproximateStoragePageSize(pageSize))
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          "1",
                          "leaf",
                          "field",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "text_value",
                          "whatever")));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field NE not-me)'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void persistenceAndEvaluateOnMissingNotMatched() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      BaseCondition condition2 = ImmutableStringCondition.of(NeFilterOperation.of(), "not-me");
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s")
              .withPageSize(configuration.getApproximateStoragePageSize(pageSize))
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          "1",
                          "leaf",
                          "field",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "text_value",
                          "not-me")));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("MERGING OR: expression '(field GT query-value | field NE not-me)'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void inMemoryAndEvaluateOnMissing() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList("query-value"), false);
      BaseCondition condition2 = ImmutableStringCondition.of(NeFilterOperation.of(), "not-me");
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s")
              .withPageSize(configuration.getApproximateStoragePageSize(pageSize))
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          "1",
                          "leaf",
                          "field",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "text_value",
                          "query-value")));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo(
                        "MERGING OR: expression '(field IN [query-value] | field NE not-me)'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void inMemoryAndEvaluateOnMissingNotMatching() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList("query-value"), false);
      BaseCondition condition2 = ImmutableStringCondition.of(NeFilterOperation.of(), "not-me");
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingDataStore.QueryAssert query1Assert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s")
              .withPageSize(configuration.getApproximateStoragePageSize(pageSize))
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          "1",
                          "leaf",
                          "field",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "text_value",
                          "not-me")));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // each query run
      query1Assert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo(
                        "MERGING OR: expression '(field IN [query-value] | field NE not-me)'");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }
  }
}
