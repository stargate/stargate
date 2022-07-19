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

package io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.OpenMocksTest;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth8TestProfile;
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

@QuarkusTest
@TestProfile(MaxDepth8TestProfile.class)
class InMemoryCandidatesFilterTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  ExecutionContext executionContext;

  @BeforeEach
  public void init() {
    executionContext = ExecutionContext.create(true);
  }

  @Nested
  class Constructor implements OpenMocksTest {

    @Mock FilterExpression filterExpression;

    @Mock BaseCondition baseCondition;

    @Test
    public void noPersistenceConditions() {
      when(baseCondition.isPersistenceCondition()).thenReturn(true);
      when(filterExpression.getCondition()).thenReturn(baseCondition);

      Throwable throwable =
          catchThrowable(
              () ->
                  InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
                      .apply(executionContext));

      assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class PrepareQuery implements OpenMocksTest {

    @Mock FilterExpression filterExpression;

    @Mock BaseCondition baseCondition;

    @Test
    public void fixedPath() {
      // fixed path has limit
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      QueryOuterClass.Query result =
          filter
              .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      String expected =
          "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND key = ? LIMIT 1 ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(result.getCql()).isEqualTo(expected);

      // execution context not updated with execution
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries()).isEmpty();
              });

      // ignore prepared as we did not execute
      resetExpectations();
    }

    @Test
    public void globComplexPath() {
      // glob path has no limits and glob px has GT as condition
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "*", "field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      QueryOuterClass.Query result =
          filter
              .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      String expected =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? AND key = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(result.getCql()).isEqualTo(expected);

      // execution context not updated with execution
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries()).isEmpty();
              });

      // ignore prepared as we did not execute
      resetExpectations();
    }
  }

  @Nested
  class BindAndFilter implements OpenMocksTest {

    @Mock RawDocument rawDocument;

    @Mock BaseCondition baseCondition;

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    @Captor ArgumentCaptor<List<RowWrapper>> testRowsCaptor;

    @Test
    public void fixedPath() {
      // fixed path has limit and page size 2 on the execution
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(filterExpression.test(anyList())).thenReturn(true);
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND key = ? LIMIT 1 ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(2)
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(List.of(List.of(Values.of("1"))));

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      filter
          .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
          .flatMap(query -> filter.bindAndFilter(queryExecutor, query, rawDocument))
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(true)
          .assertCompleted();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
      verify(rawDocument).id();
      verify(filterExpression, times(1)).test(testRowsCaptor.capture());
      verifyNoMoreInteractions(rawDocument);

      // verify that we pass the fetched document to the filter
      assertThat(testRowsCaptor.getAllValues())
          .singleElement()
          .satisfies(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          r -> {
                            assertThat(r.getString("key")).isEqualTo("1");
                          }));
    }

    @Test
    public void globPathMultipleExpressions() {
      // glob path has no limits and glob px has GT as condition
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "*", "field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field LT something");
      when(filterExpression.test(anyList())).thenReturn(true);
      when(filterExpression2.getFilterPath()).thenReturn(filterPath);
      when(filterExpression2.getCondition()).thenReturn(baseCondition);
      when(filterExpression2.getDescription()).thenReturn("field GT something");
      when(filterExpression2.test(anyList())).thenReturn(true);
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? AND key = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("some"),
                  Values.of(""),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(List.of(List.of(Values.of("1"))));

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpressions(
                  Arrays.asList(filterExpression, filterExpression2), documentProperties)
              .apply(executionContext);
      filter
          .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
          .flatMap(query -> filter.bindAndFilter(queryExecutor, query, rawDocument))
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(true)
          .assertCompleted();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("FILTER IN MEMORY: field LT something AND field GT something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
      verify(rawDocument).id();
      verify(filterExpression, times(1)).test(testRowsCaptor.capture());
      verifyNoMoreInteractions(rawDocument);

      // verify that we pass the fetched document to the filter
      assertThat(testRowsCaptor.getAllValues())
          .singleElement()
          .satisfies(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          r -> {
                            assertThat(r.getString("key")).isEqualTo("1");
                          }));
    }

    @Test
    public void nothingReturned() {
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND key = ? LIMIT 1 ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(2)
              .returningNothing();

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      filter
          .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
          .flatMap(query -> filter.bindAndFilter(queryExecutor, query, rawDocument))
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(false)
          .assertCompleted();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
      verify(rawDocument).id();
      verify(filterExpression, never()).test(testRowsCaptor.capture());
      verifyNoMoreInteractions(rawDocument);
    }

    @Test
    public void nothingReturnedButEvalOnMissing() {
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(filterExpression.test(Collections.emptyList())).thenReturn(true);
      when(baseCondition.isEvaluateOnMissingFields()).thenReturn(true);
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND key = ? LIMIT 1 ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(2)
              .returningNothing();

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      filter
          .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
          .flatMap(query -> filter.bindAndFilter(queryExecutor, query, rawDocument))
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(true)
          .assertCompleted();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
      verify(rawDocument).id();
      verify(filterExpression).test(Collections.emptyList());
      verifyNoMoreInteractions(rawDocument);
    }

    @Test
    public void testNotPassed() {
      // fixed path has limit and page size 2 on the execution
      String documentId = RandomStringUtils.randomAlphabetic(16);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(filterExpression.test(anyList())).thenReturn(false);
      when(rawDocument.id()).thenReturn(documentId);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND key = ? LIMIT 1 ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(documentId))
              .withPageSize(2)
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(List.of(List.of(Values.of("1"))));

      CandidatesFilter filter =
          InMemoryCandidatesFilter.forExpression(filterExpression, documentProperties)
              .apply(executionContext);
      filter
          .prepareQuery(KEYSPACE_NAME, COLLECTION_NAME)
          .flatMap(query -> filter.bindAndFilter(queryExecutor, query, rawDocument))
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(false)
          .assertCompleted();

      queryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("FILTER IN MEMORY: field EQ something");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
      verify(rawDocument).id();
      verify(filterExpression, times(1)).test(testRowsCaptor.capture());
      verifyNoMoreInteractions(rawDocument);

      // verify that we pass the fetched document to the filter
      assertThat(testRowsCaptor.getAllValues())
          .singleElement()
          .satisfies(
              rows ->
                  assertThat(rows)
                      .singleElement()
                      .satisfies(
                          r -> {
                            assertThat(r.getString("key")).isEqualTo("1");
                          }));
    }
  }
}
