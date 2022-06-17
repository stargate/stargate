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

import com.bpodgursky.jbool_expressions.Or;
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
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableGenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableNumberCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class OrExpressionDocumentsResolverTest extends AbstractValidatingStargateBridgeTest {

  public static final Function<List<QueryOuterClass.Value>, ByteBuffer>
      FIRST_COLUMN_COMPARABLE_KEY = row -> ByteBuffer.wrap(row.get(0).getString().getBytes());

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Nested
  class GetDocuments implements OpenMocksTest {

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
    }

    @Test
    public void twoPersistenceConditions() {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableNumberCondition.of(LtFilterOperation.of(), 1, documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(0))
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(1d))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(0))
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void twoPersistenceConditionsTwoDocuments() {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableNumberCondition.of(LtFilterOperation.of(), 1, documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(0))
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .returning(Collections.singletonList(ImmutableList.of(Values.of("2"))));

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(1d))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(0))
              .withComparableKey(FIRST_COLUMN_COMPARABLE_KEY)
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void twoPersistenceConditionsOneQueryEmpty() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableNumberCondition.of(LtFilterOperation.of(), 1, documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(Collections.singletonList(ImmutableList.of(Values.of("1"))));

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(1d))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void twoPersistenceConditionsNothingReturned() {
      int pageSize = 2;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableNumberCondition.of(LtFilterOperation.of(), 1, documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of(1d))
              .withPageSize(pageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

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
    public void persistenceAndInMemoryConditionPersistenceTrue() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList(1), documentProperties, false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("query-value"),
                          Values.NULL,
                          Values.NULL)));

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of("field"),
                          Values.NULL,
                          Values.of(2d),
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void persistenceAndInMemoryConditionMemoryTrue() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList(1), documentProperties, false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returningNothing();

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of("field"),
                          Values.NULL,
                          Values.of(1d),
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void persistenceAndInMemoryConditionMemoryFalse() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList(1), documentProperties, false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returningNothing();

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(2))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of("field"),
                          Values.NULL,
                          Values.of(2d),
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

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
    public void persistenceAndInMemoryConditionNothingReturned() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("path", "field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), Collections.singletonList(1), documentProperties, false);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value > ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("path"),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("query-value"))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returningNothing();

      ValidatingStargateBridge.QueryAssert query2Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? ALLOW FILTERING"
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME),
                  Values.of("path"),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""))
              .withPageSize(2)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returningNothing();

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

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
    public void persistenceAndEvaluateOnMissing() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableStringCondition.of(NeFilterOperation.of(), "not-me", documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("whatever"),
                          Values.NULL,
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void persistenceAndEvaluateOnMissingNotMatched() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "query-value", documentProperties);
      BaseCondition condition2 =
          ImmutableStringCondition.of(NeFilterOperation.of(), "not-me", documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("not-me"),
                          Values.NULL,
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

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
    public void inMemoryAndEvaluateOnMissing() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(),
              Collections.singletonList("query-value"),
              documentProperties,
              false);
      BaseCondition condition2 =
          ImmutableStringCondition.of(NeFilterOperation.of(), "not-me", documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("query-value"),
                          Values.NULL,
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
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
    public void inMemoryAndEvaluateOnMissingNotMatching() {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(),
              Collections.singletonList("query-value"),
              documentProperties,
              false);
      BaseCondition condition2 =
          ImmutableStringCondition.of(NeFilterOperation.of(), "not-me", documentProperties);
      ImmutableFilterExpression filterExpression1 =
          ImmutableFilterExpression.of(filterPath, condition, 0);
      ImmutableFilterExpression filterExpression2 =
          ImmutableFilterExpression.of(filterPath, condition2, 1);

      ValidatingStargateBridge.QueryAssert query1Assert =
          withQuery(
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
                      .formatted(KEYSPACE_NAME, COLLECTION_NAME))
              .withPageSize(documentProperties.getApproximateStoragePageSize(pageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpecForPathDepth(3))
              .returning(
                  Collections.singletonList(
                      ImmutableList.of(
                          Values.of("1"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("not-me"),
                          Values.NULL,
                          Values.NULL)));

      Or<FilterExpression> or = Or.of(filterExpression1, filterExpression2);
      DocumentsResolver resolver =
          new OrExpressionDocumentsResolver(or, executionContext, documentProperties);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

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
