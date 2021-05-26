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
import static org.mockito.Mockito.lenient;

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
    lenient().when(configuration.getSearchPageSize()).thenReturn(100);
  }

  @Nested
  class SearchDocuments {

    @Test
    public void happyPath() {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql)
              .withPageSize(configuration.getSearchPageSize())
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      String populateCql =
          "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of(
                          "key", "1", "text_value", "other", "p0", "another", "p1", "field")));

      ValidatingDataStore.QueryAssert populateSecondAssert =
          withQuery(TABLE, populateCql, "2")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "2", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of("key", "2", "text_value", "other2", "p0", "another2")));

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore()),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              Collections.emptyList(),
              paginator,
              context);

      // assert results
      results
          .test()
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
    public void limitedResults() {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql)
              .withPageSize(configuration.getSearchPageSize())
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      String populateCql =
          "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getSearchPageSize())
              .returning(
                  Arrays.asList(
                      ImmutableMap.of(
                          "key", "1", "text_value", "find-me", "p0", "some", "p1", "field"),
                      ImmutableMap.of(
                          "key", "1", "text_value", "other", "p0", "another", "p1", "field")));

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore()),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              Collections.emptyList(),
              paginator,
              context);

      // assert results
      results
          .test()
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
    public void nothingToPopulate() {
      Paginator paginator = new Paginator(null, 1);
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("some", "field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition);

      String candidatesCql =
          "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND leaf = ? AND p2 = ? AND text_value = ? ALLOW FILTERING";
      ValidatingDataStore.QueryAssert candidatesAssert =
          withQuery(TABLE, candidatesCql)
              .withPageSize(configuration.getSearchPageSize())
              .returningNothing();

      String populateCql =
          "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s WHERE key = ?";
      ValidatingDataStore.QueryAssert populateFirstAssert =
          withQuery(TABLE, populateCql, "1")
              .withPageSize(configuration.getSearchPageSize())
              .returningNothing();

      Flowable<RawDocument> results =
          service.searchDocuments(
              new QueryExecutor(datastore()),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              expression,
              Collections.emptyList(),
              paginator,
              context);

      // assert results
      results.test().assertValueCount(0).assertComplete();

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

      ignorePreparedExecutions();
    }

    @Test
    public void fullSearch() {
      Paginator paginator = new Paginator(null, 20);
      ExecutionContext context = ExecutionContext.create(true);

      String searchCql =
          "SELECT key, leaf, text_value, dbl_value, bool_value, p0, p1, p2, p3, WRITETIME(leaf) FROM %s";
      ValidatingDataStore.QueryAssert searchAssert =
          withQuery(TABLE, searchCql)
              .withPageSize(configuration.getSearchPageSize())
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
              new QueryExecutor(datastore()),
              KEYSPACE_NAME,
              COLLECTION_NAME,
              Literal.getTrue(),
              Collections.emptyList(),
              paginator,
              context);

      // assert results
      results
          .test()
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
}
