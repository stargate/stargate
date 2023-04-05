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

package io.stargate.web.docsapi.service.query.search.resolver.filter.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.ValidatingDataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableNumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PersistenceCandidatesFilterTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(8);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  DocsApiConfiguration configuration;

  @Mock FilterExpression filterExpression;

  QueryExecutor queryExecutor;

  ExecutionContext executionContext;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @BeforeEach
  public void init() {
    executionContext = ExecutionContext.create(true);
    queryExecutor = new QueryExecutor(datastore(), configuration);
    lenient().when(configuration.getMaxStoragePageSize()).thenReturn(100);
  }

  @Nested
  class Constructor {

    @Mock BaseCondition baseCondition;

    @Test
    public void noInMemoryConditions() {
      when(baseCondition.isPersistenceCondition()).thenReturn(false);
      when(filterExpression.getCondition()).thenReturn(baseCondition);

      Throwable throwable =
          catchThrowable(
              () ->
                  PersistenceCandidatesFilter.forExpression(filterExpression, configuration)
                      .apply(executionContext));

      assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class PrepareQuery {

    @Test
    public void fixedPath() throws Exception {
      // fixed path has limit
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      withQuery(
          TABLE,
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? AND key = ? LIMIT ? ALLOW FILTERING");

      CandidatesFilter filter =
          PersistenceCandidatesFilter.forExpression(filterExpression, configuration)
              .apply(executionContext);
      Single<? extends Query<? extends BoundQuery>> single =
          filter.prepareQuery(datastore(), KEYSPACE_NAME, COLLECTION_NAME);

      single.test().await().assertValueCount(1).assertComplete();

      // execution context not updated with execution
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER: field EQ something");
                assertThat(nested.queries()).isEmpty();
              });

      // ignore prepared as we did not execute
      resetExpectations();
    }

    @Test
    public void globComplexPath() throws Exception {
      // glob path has no limits and glob px has GT as condition
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "*", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      withQuery(
          TABLE,
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? AND text_value = ? AND key = ? ALLOW FILTERING");

      CandidatesFilter filter =
          PersistenceCandidatesFilter.forExpression(filterExpression, configuration)
              .apply(executionContext);
      Single<? extends Query<? extends BoundQuery>> single =
          filter.prepareQuery(datastore(), KEYSPACE_NAME, COLLECTION_NAME);

      single.test().await().assertValueCount(1).assertComplete();

      // execution context not updated with execution
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER: field EQ something");
                assertThat(nested.queries()).isEmpty();
              });

      // ignore prepared as we did not execute
      resetExpectations();
    }
  }

  @Nested
  class BindAndFilter {

    @Mock RawDocument rawDocument;

    @Mock FilterExpression filterExpression2;

    @Test
    public void fixedPath() throws Exception {
      // fixed path has limit and page size 2 on the execution
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? AND key = ? LIMIT ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value",
                  documentId,
                  1)
              .withPageSize(2)
              .returning(Arrays.asList(ImmutableMap.of("key", "1")));

      CandidatesFilter filter =
          PersistenceCandidatesFilter.forExpression(filterExpression, configuration)
              .apply(executionContext);
      Query<? extends BoundQuery> query =
          filter.prepareQuery(datastore(), KEYSPACE_NAME, COLLECTION_NAME).blockingGet();
      Maybe<?> result = filter.bindAndFilter(queryExecutor, query, rawDocument);

      result.test().await().assertValueCount(1).assertComplete();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER: field EQ something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
      verify(rawDocument).id();
      verifyNoMoreInteractions(rawDocument);
    }

    @Test
    public void globPathMultipleExpressions() throws Exception {
      // glob path has no limits and glob px has GT as condition
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "*", "field"));
      BaseCondition condition = ImmutableNumberCondition.of(LtFilterOperation.of(), 1d);
      BaseCondition condition2 = ImmutableNumberCondition.of(GtFilterOperation.of(), 2d);
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field LT something");
      when(filterExpression2.getFilterPath()).thenReturn(filterPath);
      when(filterExpression2.getCondition()).thenReturn(condition2);
      when(filterExpression2.getDescription()).thenReturn("field GT something");
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? AND dbl_value < ? AND dbl_value > ? AND key = ? ALLOW FILTERING",
                  "some",
                  "",
                  "field",
                  "field",
                  "",
                  1.0,
                  2.0,
                  documentId)
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(Arrays.asList(ImmutableMap.of("key", "1")));

      CandidatesFilter filter =
          PersistenceCandidatesFilter.forExpressions(
                  Arrays.asList(filterExpression, filterExpression2), configuration)
              .apply(executionContext);
      Query<? extends BoundQuery> query =
          filter.prepareQuery(datastore(), KEYSPACE_NAME, COLLECTION_NAME).blockingGet();
      Maybe<?> result = filter.bindAndFilter(queryExecutor, query, rawDocument);

      result.test().await().assertValueCount(1).assertComplete();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("FILTER: field LT something AND field GT something");
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
    public void nothingReturned() throws Exception {
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? AND key = ? LIMIT ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value",
                  documentId,
                  1)
              .withPageSize(2)
              .returningNothing();

      CandidatesFilter filter =
          PersistenceCandidatesFilter.forExpression(filterExpression, configuration)
              .apply(executionContext);
      Query<? extends BoundQuery> query =
          filter.prepareQuery(datastore(), KEYSPACE_NAME, COLLECTION_NAME).blockingGet();
      Maybe<?> result = filter.bindAndFilter(queryExecutor, query, rawDocument);

      result.test().await().assertValueCount(0).assertComplete();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER: field EQ something");
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
