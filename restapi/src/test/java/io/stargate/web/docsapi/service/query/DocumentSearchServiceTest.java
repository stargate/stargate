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

package io.stargate.web.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.ValidatingDataStore;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.models.ExecutionProfile;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentSearchServiceTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 4;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @InjectMocks DocumentSearchService service;

  @Mock DocsApiConfiguration configuration;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @BeforeEach
  public void init() {
    lenient().when(configuration.getMaxDepth()).thenReturn(MAX_DEPTH);
    lenient().when(configuration.getMaxStoragePageSize()).thenReturn(100);
    lenient().when(configuration.getApproximateStoragePageSize(anyInt())).thenCallRealMethod();
  }

  @Nested
  class SearchDocuments {

    @Test
    public void happyPath() throws Exception {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql, "some", "field", "field", "", "find-me")
              .withPageSize(paginator.docPageSize + 1)
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of(
                          "key", "1", "text_value", "other", "p0", "another", "p1", "field")));

      ValidatingDataStore.QueryAssert populateSecondAssert =
          withQuery(TABLE, populateCql, "2")
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "2", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of("key", "2", "text_value", "other2", "p0", "another2")));

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValueAt(
              0,
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
                return true;
              })
          .assertValueAt(
              1,
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
                return true;
              })
          .assertComplete();

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
    public void limitedResults() throws Exception {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql, "some", "field", "field", "", "find-me")
              .withPageSize(paginator.docPageSize + 1)
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of(
                          "key", "1", "text_value", "other", "p0", "another", "p1", "field")));

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValueAt(
              0,
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
                return true;
              })
          .assertComplete();

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
    public void nothingToPopulate() throws Exception {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql, "some", "field", "field", "", "find-me")
              .withPageSize(paginator.docPageSize + 1)
              .returningNothing();

      String populateCql =
          "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getMaxStoragePageSize())
              .returningNothing();

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              paginator,
              context);

      // assert results
      results.test().await().assertValueCount(0).assertComplete();

      // assert queries execution
      candidatesAssert.assertExecuteCount().isEqualTo(1);
      populateFirstAssert.assertExecuteCount().isEqualTo(0);

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

      resetExpectations();
    }

    @Test
    public void fullSearch() throws Exception {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);

      String searchCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s";
      ValidatingDataStore.QueryAssert searchAssert =
          withQuery(TABLE, searchCql)
              .withPageSize(
                  configuration.getApproximateStoragePageSize(
                      paginator.docPageSize)) // see DocsConfiguration
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of(
                          "key", "1", "text_value", "other", "p0", "another", "p1", "field"),
                      ImmutableMap.of(
                          "key", "2", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of("key", "2", "text_value", "other2", "p0", "another2")));

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              Literal.getTrue(),
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValueAt(
              0,
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
                return true;
              })
          .assertValueAt(
              1,
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
                return true;
              })
          .assertComplete();

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
    public void searchFullDoc() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert cqlAssert =
          withQuery(TABLE, cql, documentId)
              .withPageSize(configuration.getApproximateStoragePageSize(paginator.docPageSize))
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key",
                          documentId,
                          "text_value",
                          "find-me",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "leaf",
                          "field"),
                      ImmutableMap.of(
                          "key",
                          documentId,
                          "text_value",
                          "other",
                          "p0",
                          "another",
                          "p1",
                          "",
                          "leaf",
                          "another")));

      Flowable<RawDocument> results =
          service.searchSubDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              Collections.emptyList(),
              expression,
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValue(
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
                return true;
              })
          .assertComplete();

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
    public void searchSubDoc() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 10);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(subPath);
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert cqlAssert =
          withQuery(TABLE, cql, "field", documentId)
              .withPageSize(configuration.getApproximateStoragePageSize(paginator.docPageSize))
              .returning(
                  Collections.singletonList(
                      ImmutableMap.of(
                          "key",
                          documentId,
                          "text_value",
                          "find-me",
                          "p0",
                          "field",
                          "p1",
                          "",
                          "leaf",
                          "field")));

      Flowable<RawDocument> results =
          service.searchSubDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              subPath,
              expression,
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValue(
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
                return true;
              })
          .assertComplete();

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
    public void searchSubDocPaginated() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      Paginator paginator = new Paginator(null, 2);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("*");

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 > ? AND key = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert cqlAssert =
          withQuery(TABLE, cql, "", documentId)
              .withPageSize(configuration.getApproximateStoragePageSize(paginator.docPageSize))
              .returning(
                  Arrays.asList(
                      ImmutableMap.of("key", documentId, "text_value", "v1", "p0", "field1"),
                      ImmutableMap.of("key", documentId, "text_value", "v2", "p0", "field2"),
                      ImmutableMap.of("key", documentId, "text_value", "v3", "p0", "field3")));

      Flowable<RawDocument> results =
          service.searchSubDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              subPath,
              Literal.getTrue(),
              paginator,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValueAt(
              0,
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .singleElement()
                    .satisfies(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("p0")).isEqualTo("field1");
                        });
                return true;
              })
          .assertValueAt(
              1,
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows())
                    .singleElement()
                    .satisfies(
                        r -> {
                          assertThat(r.getString("key")).isEqualTo(documentId);
                          assertThat(r.getString("p0")).isEqualTo("field2");
                        });
                return true;
              })
          .assertComplete();

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
    public void getSubDoc() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      List<String> subPath = Collections.singletonList("field");

      String cql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND key = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert cqlAssert =
          withQuery(TABLE, cql, "field", documentId)
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", documentId, "text_value", "v1", "p0", "field", "p1", "k1"),
                      ImmutableMap.of(
                          "key", documentId, "text_value", "v2", "p0", "field", "p1", "k2"),
                      ImmutableMap.of(
                          "key", documentId, "text_value", "v3", "p0", "field", "p1", "k3")));

      Flowable<RawDocument> results =
          service.getDocument(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              subPath,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows()).hasSize(3);
                return true;
              })
          .assertComplete();

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
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      String cql = "SELECT key, TTL(leaf), WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert cqlAssert =
          withQuery(TABLE, cql, documentId)
              .withPageSize(configuration.getMaxStoragePageSize())
              .returning(Arrays.asList(ImmutableMap.of("key", documentId, "ttl(leaf)", 0)));

      Flowable<RawDocument> results =
          service.getDocumentTtlInfo(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              context);

      // assert results
      results
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo(documentId);
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

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
    public void predicateOrderWithExplicitSelectivity() throws Exception {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath1 = ImmutableFilterPath.of(Arrays.asList("some", "field1"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Arrays.asList("some", "field2"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      Expression<FilterExpression> expression =
          And.of(
              // the filter on field2 is listed first but with worse selectivity
              FilterExpression.of(filterPath2, condition, 0, 0.9),
              // the filter on field1 is listed second but with better selectivity
              FilterExpression.of(filterPath1, condition, 1, 0.5));

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      withQuery(TABLE, candidatesCql, "some", "field1", "field1", "", "find-me")
          .withPageSize(paginator.docPageSize + 1)
          .returning(Collections.singletonList(ImmutableMap.of("key", "1")));
      String filterCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? AND key = ? LIMIT ? ALLOW FILTERING";
      withQuery(TABLE, filterCql, "some", "field2", "field2", "", "find-me", "1", 1)
          .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      String populateCql =
          "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE key = ?";

      withQuery(TABLE, populateCql, "1")
          .withPageSize(configuration.getMaxStoragePageSize())
          .returning(
              Arrays.asList(
                  ImmutableMap.of("key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                  ImmutableMap.of(
                      "key", "1", "text_value", "other", "p0", "another", "p1", "field")));

      service
          .searchDocuments(
              new QueryExecutor(datastore(), configuration),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              paginator,
              context)
          .test()
          .await()
          .assertNoErrors();

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
