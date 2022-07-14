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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.OpenMocksTest;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth8TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@QuarkusTest
@TestProfile(MaxDepth8TestProfile.class)
class InMemoryDocumentsResolverTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

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
              () -> new InMemoryDocumentsResolver(filterExpression, null, documentProperties));

      assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class GetDocuments implements OpenMocksTest {

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    @Mock BaseCondition baseCondition;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      when(baseCondition.isPersistenceCondition()).thenReturn(false);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
    }

    @Test
    public void happyPath() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
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
                assertThat(doc.rows()).hasSize(1);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());

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
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void happyPathEvalOnMissing() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      when(baseCondition.isEvaluateOnMissingFields()).thenReturn(true);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
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
                assertThat(doc.rows()).hasSize(1);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());

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
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void happyPathMultipleExpressions() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getDescription()).thenReturn("field EQ something");
      when(filterExpression2.getFilterPath()).thenReturn(filterPath);
      when(filterExpression2.getCondition()).thenReturn(baseCondition);
      when(filterExpression2.getDescription()).thenReturn("field GT something");
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);
      when(filterExpression2.test(Mockito.<RawDocument>any())).thenReturn(false);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(
              Arrays.asList(filterExpression, filterExpression2),
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
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("FILTER IN MEMORY: field EQ something AND field GT something");
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
    public void multipleDocuments() {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      ImmutableList.of(Values.of("1")), ImmutableList.of(Values.of("2"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
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
                assertThat(doc.rows()).hasSize(1);
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.rows()).hasSize(1);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(2)).test(Mockito.<RawDocument>any());

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested ->
                  assertThat(nested.queries())
                      .singleElement()
                      .satisfies(
                          queryInfo -> {
                            assertThat(queryInfo.execCount()).isEqualTo(1);
                            assertThat(queryInfo.rowCount()).isEqualTo(2);
                          }));
    }

    @Test
    public void nothingReturnedFromDataStore() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(0)).test(Mockito.<RawDocument>any());
    }

    @Test
    public void complexFilterPath() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("field", "nested", "value"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("nested"),
                  Values.of("value"),
                  Values.of("value"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
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
                assertThat(doc.rows()).hasSize(1);
              });

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());
    }

    @Test
    public void testNotPassed() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(false);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingStargateBridge.QueryAssert queryAssert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());

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
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }
  }
}
