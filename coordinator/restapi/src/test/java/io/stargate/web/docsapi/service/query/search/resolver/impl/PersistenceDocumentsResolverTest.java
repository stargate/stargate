package io.stargate.web.docsapi.service.query.search.resolver.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableNumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GteFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PersistenceDocumentsResolverTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(8);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class Constructor {

    @Mock FilterExpression filterExpression;

    @Mock BaseCondition baseCondition;

    @Mock DocsApiConfiguration configuration;

    @Test
    public void noInMemoryConditions() {
      when(baseCondition.isPersistenceCondition()).thenReturn(false);
      when(filterExpression.getCondition()).thenReturn(baseCondition);

      Throwable throwable =
          catchThrowable(
              () -> new PersistenceDocumentsResolver(filterExpression, null, configuration));

      assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class GetDocuments {

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    DocsApiConfiguration configuration;

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    QueryExecutor queryExecutor;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      queryExecutor = new QueryExecutor(datastore(), configuration);
    }

    @Test
    public void happyPath() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(GtFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value > ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new PersistenceDocumentsResolver(filterExpression, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .take(1)
          .test()
          .await()
          .assertValue(
              doc -> {
                assertThat(doc.id()).isEqualTo("1");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(Mockito.<RawDocument>any());

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
    }

    @Test
    public void happyPathMultipleExpressions() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableNumberCondition.of(GteFilterOperation.of(), 1d);
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(filterExpression.getDescription()).thenReturn("field LTE something");
      BaseCondition condition2 = ImmutableNumberCondition.of(LteFilterOperation.of(), 2d);
      when(filterExpression2.getFilterPath()).thenReturn(filterPath);
      when(filterExpression2.getCondition()).thenReturn(condition2);
      when(filterExpression2.getDescription()).thenReturn("field GTE something");

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value >= ? AND dbl_value <= ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  1.0,
                  2.0)
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new PersistenceDocumentsResolver(
              Arrays.asList(filterExpression, filterExpression2), executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(Mockito.<RawDocument>any());

      // execution context
      assertThat(executionContext.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description())
                    .isEqualTo("FILTER: field LTE something AND field GTE something");
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
    public void multipleDocuments() throws Exception {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(LtFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value < ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      DocumentsResolver resolver =
          new PersistenceDocumentsResolver(filterExpression, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result
          .test()
          .await()
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
                assertThat(doc.id()).isEqualTo("2");
                assertThat(doc.rows()).hasSize(1);
                return true;
              })
          .assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(Mockito.<RawDocument>any());

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
    public void nothingReturnedFromDataStore() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returningNothing();

      DocumentsResolver resolver =
          new PersistenceDocumentsResolver(filterExpression, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertNoValues().assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(Mockito.<RawDocument>any());
    }

    @Test
    public void complexFilterPath() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("field", "nested", "value"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "query-value");
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, leaf, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND p2 = ? AND leaf = ? AND p3 = ? AND text_value = ? ALLOW FILTERING",
                  "field",
                  "nested",
                  "value",
                  "value",
                  "",
                  "query-value")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new PersistenceDocumentsResolver(filterExpression, executionContext, configuration);
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

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, never()).test(Mockito.<RawDocument>any());
    }
  }
}
