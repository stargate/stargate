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

package io.stargate.sgv2.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class ReadBridgeServiceTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject ReadBridgeService service;

  @Inject DocumentProperties documentProperties;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Nested
  class SearchDocuments {

    @Test
    public void happyPath() {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  candidatesCql,
                  Values.of("some"),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("find-me"))
              .withPageSize(paginator.docPageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(List.of(List.of(Values.of("1")), List.of(Values.of("2"))));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE key = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert populateFirstAssert =
          withQuery(populateCql, Values.of("1"))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      List.of(
                          Values.of("1"),
                          Values.of("some"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("1"),
                          Values.of("another"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("other"),
                          Values.NULL,
                          Values.NULL)));

      ValidatingStargateBridge.QueryAssert populateSecondAssert =
          withQuery(populateCql, Values.of("2"))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      List.of(
                          Values.of("2"),
                          Values.of("some"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("2"),
                          Values.of("another2"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("other2"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchDocuments(KEYSPACE_NAME, COLLECTION_NAME, expression, paginator, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result).hasSize(2);
      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("some");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("other");
                          assertThat(r.getString("p0")).isEqualTo("another");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        });
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("2");
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("some");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("2");
                          assertThat(r.getString("text_value")).isEqualTo("other2");
                          assertThat(r.getString("p0")).isEqualTo("another2");
                        });
              });

      // assert queries execution
      candidatesAssert.assertExecuteCount().isEqualTo(1);
      populateFirstAssert.assertExecuteCount().isEqualTo(1);
      populateSecondAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(2)
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("FILTER: some.field EQ find-me");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      candidatesCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              })
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("LoadProperties");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(4);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      populateCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }

    @Test
    public void limitedResults() {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  candidatesCql,
                  Values.of("some"),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("find-me"))
              .withPageSize(paginator.docPageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(List.of(List.of(Values.of("1")), List.of(Values.of("2"))));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE key = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert populateFirstAssert =
          withQuery(populateCql, Values.of("1"))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      List.of(
                          Values.of("1"),
                          Values.of("some"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("1"),
                          Values.of("another"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("other"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchDocuments(KEYSPACE_NAME, COLLECTION_NAME, expression, paginator, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("some");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("other");
                          assertThat(r.getString("p0")).isEqualTo("another");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        });
              });

      // assert queries execution
      candidatesAssert.assertExecuteCount().isEqualTo(1);
      populateFirstAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(2)
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("FILTER: some.field EQ find-me");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      candidatesCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              })
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("LoadProperties");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      populateCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }

    @Test
    public void nothingToPopulate() {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  candidatesCql,
                  Values.of("some"),
                  Values.of("field"),
                  Values.of("field"),
                  Values.of(""),
                  Values.of("find-me"))
              .withPageSize(paginator.docPageSize + 1)
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returningNothing();

      service
          .searchDocuments(KEYSPACE_NAME, COLLECTION_NAME, expression, paginator, context)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(100))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      // assert queries execution
      candidatesAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(2)
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("FILTER: some.field EQ find-me");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      candidatesCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              })
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("LoadProperties");
                assertThat(c.queries()).isEmpty();
              });
    }

    @Test
    public void fullSearch() {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);

      String searchCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\""
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert searchAssert =
          withQuery(searchCql)
              .withPageSize(documentProperties.getApproximateStoragePageSize(paginator.docPageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      List.of(
                          Values.of("1"),
                          Values.of("some"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("1"),
                          Values.of("another"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("other"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("2"),
                          Values.of("some"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of("2"),
                          Values.of("another2"),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("other2"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchDocuments(
                  KEYSPACE_NAME, COLLECTION_NAME, Literal.getTrue(), paginator, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result).hasSize(2);
      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("some");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("1");
                          assertThat(r.getString("text_value")).isEqualTo("other");
                          assertThat(r.getString("p0")).isEqualTo("another");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        });
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("2");
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("some");
                          assertThat(r.getString("p1")).isEqualTo("field");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo("2");
                          assertThat(r.getString("text_value")).isEqualTo("other2");
                          assertThat(r.getString("p0")).isEqualTo("another2");
                        });
              });

      // assert queries execution
      searchAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(1)
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("LoadAllDocuments");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(4);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(searchCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }
  }

  @Nested
  class SearchSubDocuments {

    @Test
    public void searchFullDoc() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE key = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert cqlAssert =
          withQuery(cql, Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(paginator.docPageSize))
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  Arrays.asList(
                      List.of(
                          Values.of(documentId),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(documentId),
                          Values.of("another"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("another"),
                          Values.of("other"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchSubDocuments(
                  KEYSPACE_NAME,
                  COLLECTION_NAME,
                  documentId,
                  Collections.emptyList(),
                  expression,
                  paginator,
                  context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .hasSize(2)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("field");
                          assertThat(r.getString("p1")).isEqualTo("");
                        })
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("text_value")).isEqualTo("other");
                          assertThat(r.getString("p0")).isEqualTo("another");
                          assertThat(r.getString("p1")).isEqualTo("");
                        });
              });

      // assert queries execution
      cqlAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(1)
          .anySatisfy(
              c -> {
                assertThat(c.description())
                    .isEqualTo("SearchSubDocuments: sub-path '', expression: 'field EQ find-me'");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(String.format(cql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }

    @Test
    public void searchSubDoc() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 10);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(subPath);
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND key = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert cqlAssert =
          withQuery(cql, Values.of("field"), Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(paginator.docPageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  List.of(
                      List.of(
                          Values.of(documentId),
                          Values.of("field"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field"),
                          Values.of("find-me"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchSubDocuments(
                  KEYSPACE_NAME,
                  COLLECTION_NAME,
                  documentId,
                  subPath,
                  expression,
                  paginator,
                  context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .hasSize(1)
                    .anySatisfy(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("text_value")).isEqualTo("find-me");
                          assertThat(r.getString("p0")).isEqualTo("field");
                          assertThat(r.getString("p1")).isEqualTo("");
                        });
              });

      // assert queries execution
      cqlAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(1)
          .anySatisfy(
              c -> {
                assertThat(c.description())
                    .isEqualTo(
                        "SearchSubDocuments: sub-path 'field', expression: 'field EQ find-me'");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(String.format(cql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }

    @Test
    public void searchSubDocPaginated() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 2);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("*");

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 > ? AND key = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert cqlAssert =
          withQuery(cql, Values.of(""), Values.of(documentId))
              .withPageSize(documentProperties.getApproximateStoragePageSize(paginator.docPageSize))
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  List.of(
                      List.of(
                          Values.of(documentId),
                          Values.of("field1"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field1"),
                          Values.of("v1"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(documentId),
                          Values.of("field2"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field2"),
                          Values.of("v2"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(documentId),
                          Values.of("field3"),
                          Values.of(""),
                          Values.of(""),
                          Values.of(""),
                          Values.of("field3"),
                          Values.of("v3"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .searchSubDocuments(
                  KEYSPACE_NAME,
                  COLLECTION_NAME,
                  documentId,
                  subPath,
                  Literal.getTrue(),
                  paginator,
                  context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result).hasSize(2);
      assertThat(result.get(0))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .singleElement()
                    .satisfies(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("p0")).isEqualTo("field1");
                        });
              });
      assertThat(result.get(1))
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .singleElement()
                    .satisfies(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("p0")).isEqualTo("field2");
                        });
              });

      // assert queries execution
      cqlAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(1)
          .anySatisfy(
              c -> {
                assertThat(c.description())
                    .isEqualTo("SearchSubDocuments: sub-path '*', expression: 'true'");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(3);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(String.format(cql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }
  }

  @Nested
  class GetDocument {

    @Test
    public void getSubDoc() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("field");

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND key = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert cqlAssert =
          withQuery(cql, Values.of("field"), Values.of(documentId))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withResumeMode(QueryOuterClass.ResumeMode.NEXT_ROW)
              .enriched()
              .withColumnSpec(schemaProvider.allColumnSpec())
              .returning(
                  List.of(
                      List.of(
                          Values.of(documentId),
                          Values.of("field"),
                          Values.of("k1"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("k1"),
                          Values.of("v1"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(documentId),
                          Values.of("field"),
                          Values.of("k2"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("k2"),
                          Values.of("v2"),
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(documentId),
                          Values.of("field"),
                          Values.of("k3"),
                          Values.of(""),
                          Values.of(""),
                          Values.of("k3"),
                          Values.of("v3"),
                          Values.NULL,
                          Values.NULL)));

      List<RawDocument> result =
          service
              .getDocument(KEYSPACE_NAME, COLLECTION_NAME, documentId, subPath, context)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(100))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      // assert results
      assertThat(result)
          .singleElement()
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows()).hasSize(3);
              });

      // assert queries execution
      cqlAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested())
          .hasSize(1)
          .anySatisfy(
              c -> {
                assertThat(c.description()).isEqualTo("GetFullDocument");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(3);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(String.format(cql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }
  }

  @Nested
  class GetDocumentTtlInfo {

    @Test
    public void happyPath() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      String cql =
          "SELECT key, TTL(leaf), WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE key = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert cqlAssert =
          withQuery(cql, Values.of(documentId))
              .withPageSize(documentProperties.maxSearchPageSize())
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder().setName("key").build(),
                      QueryOuterClass.ColumnSpec.newBuilder().setName("ttl(leaf)").build()))
              .returning(List.of(List.of(Values.of(documentId), Values.of(100))));

      RawDocument result =
          service
              .getDocumentTtlInfo(KEYSPACE_NAME, COLLECTION_NAME, documentId, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .assertCompleted()
              .getItem();

      // assert results
      assertThat(result)
          .satisfies(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows()).hasSize(1);
              });

      // assert queries execution
      cqlAssert.assertExecuteCount().isEqualTo(1);

      // assert execution context
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.queries())
          .singleElement()
          .satisfies(
              queryInfo -> {
                assertThat(queryInfo.execCount()).isEqualTo(1);
                assertThat(queryInfo.rowCount()).isEqualTo(1);
                assertThat(queryInfo.preparedCQL())
                    .isEqualTo(String.format(cql, KEYSPACE_NAME + "." + COLLECTION_NAME));
              });
    }
  }

  @Nested
  class SelectivityHints {

    @Test
    public void predicateOrderWithExplicitSelectivity() {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath1 = ImmutableFilterPath.of(Arrays.asList("some", "field1"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Arrays.asList("some", "field2"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      Expression<FilterExpression> expression =
          And.of(
              // the filter on field2 is listed first but with worse selectivity
              FilterExpression.of(filterPath2, condition, 0, 0.9),
              // the filter on field1 is listed second but with better selectivity
              FilterExpression.of(filterPath1, condition, 1, 0.5));

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      withQuery(
              candidatesCql,
              Values.of("some"),
              Values.of("field1"),
              Values.of("field1"),
              Values.of(""),
              Values.of("find-me"))
          .withPageSize(paginator.docPageSize + 1)
          .withResumeMode(QueryOuterClass.ResumeMode.NEXT_PARTITION)
          .enriched()
          .withColumnSpec(schemaProvider.allColumnSpec())
          .returning(Collections.singletonList(List.of(Values.of("1"))));

      String filterCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? AND key = ? LIMIT 1 ALLOW FILTERING"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      withQuery(
              filterCql,
              Values.of("some"),
              Values.of("field2"),
              Values.of("field2"),
              Values.of(""),
              Values.of("find-me"),
              Values.of("1"))
          .withColumnSpec(schemaProvider.allColumnSpec())
          .returning(Collections.singletonList(List.of(Values.of("1"))));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE key = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      withQuery(populateCql, Values.of("1"))
          .withPageSize(documentProperties.maxSearchPageSize())
          .withColumnSpec(schemaProvider.allColumnSpec())
          .returning(
              Arrays.asList(
                  List.of(
                      Values.of("1"),
                      Values.of("some"),
                      Values.of("field"),
                      Values.of(""),
                      Values.of(""),
                      Values.of("field"),
                      Values.of("find-me"),
                      Values.NULL,
                      Values.NULL),
                  List.of(
                      Values.of("1"),
                      Values.of("another"),
                      Values.of("field"),
                      Values.of(""),
                      Values.of(""),
                      Values.of("field"),
                      Values.of("other"),
                      Values.NULL,
                      Values.NULL)));

      service
          .searchDocuments(KEYSPACE_NAME, COLLECTION_NAME, expression, paginator, context)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(100))
          .awaitItems(1)
          .awaitCompletion()
          .assertCompleted();

      // Rely on profiling output to validate the order of filter execution
      ExecutionProfile executionProfile = context.toProfile();
      assertThat(executionProfile.nested()).hasSize(3);

      assertThat(executionProfile.nested())
          .element(0)
          .satisfies(
              c -> {
                // Note: the filter on field1 becomes the top filter due to its lower selectivity
                assertThat(c.description()).isEqualTo("FILTER: some.field1 EQ find-me");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      candidatesCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });

      assertThat(executionProfile.nested())
          .element(1)
          .satisfies(
              c -> {
                assertThat(c.description()).isEqualTo("PARALLEL [ALL OF]");
                assertThat(c.nested())
                    .singleElement()
                    .satisfies(
                        c1 -> {
                          // Note: the filter on field2 becomes the nested filter due to its worse
                          // selectivity
                          assertThat(c1.description()).isEqualTo("FILTER: some.field2 EQ find-me");
                          assertThat(c1.queries())
                              .singleElement()
                              .satisfies(
                                  queryInfo -> {
                                    assertThat(queryInfo.execCount()).isEqualTo(1);
                                    assertThat(queryInfo.rowCount()).isEqualTo(1);
                                    assertThat(queryInfo.preparedCQL())
                                        .isEqualTo(
                                            String.format(
                                                filterCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                                  });
                        });
              });

      assertThat(executionProfile.nested())
          .element(2)
          .satisfies(
              c -> {
                assertThat(c.description()).isEqualTo("LoadProperties");
                assertThat(c.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      populateCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                        });
              });
    }
  }
}
