package io.stargate.web.docsapi.service.query.search.resolver.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
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
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InMemoryDocumentsResolverTest extends AbstractDataStoreTest {

  private static final Integer MAX_DEPTH = 8;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class Constructor {

    @Mock DocsApiConfiguration configuration;

    @Mock FilterExpression filterExpression;

    @Mock BaseCondition baseCondition;

    @Test
    public void noPersistenceConditions() {
      when(baseCondition.isPersistenceCondition()).thenReturn(true);
      when(filterExpression.getCondition()).thenReturn(baseCondition);

      Throwable throwable =
          catchThrowable(
              () -> new InMemoryDocumentsResolver(filterExpression, null, configuration));

      assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class GetDocuments {

    @Mock DocsApiConfiguration configuration;

    @Mock FilterExpression filterExpression;

    @Mock FilterExpression filterExpression2;

    @Mock BaseCondition baseCondition;

    QueryExecutor queryExecutor;

    ExecutionContext executionContext;

    @BeforeEach
    public void init() {
      executionContext = ExecutionContext.create(true);
      queryExecutor = new QueryExecutor(datastore(), configuration);
      lenient().when(configuration.getApproximateStoragePageSize(anyInt())).thenCallRealMethod();
      when(configuration.getMaxDepth()).thenReturn(MAX_DEPTH);
      when(configuration.getMaxStoragePageSize()).thenReturn(1000);
      when(baseCondition.isPersistenceCondition()).thenReturn(false);
      when(filterExpression.getCondition()).thenReturn(baseCondition);
    }

    @Test
    public void happyPath() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
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
    public void happyPathEvalOnMissing() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      when(baseCondition.isEvaluateOnMissingFields()).thenReturn(true);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s")
              .withPageSize(configuration.getApproximateStoragePageSize(pageSize))
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
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
    public void happyPathMultipleExpressions() throws Exception {
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

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(
              Arrays.asList(filterExpression, filterExpression2), executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertComplete();

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
    public void multipleDocuments() throws Exception {
      int pageSize = 10;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(pageSize + 1)
              .returning(Arrays.asList(ImmutableMap.of("key", "1"), ImmutableMap.of("key", "2")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
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
    public void nothingReturnedFromDataStore() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(pageSize + 1)
              .returningNothing();

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertNoValues().assertComplete();

      // one query only
      queryAssert.assertExecuteCount().isEqualTo(1);
      verify(filterExpression, times(0)).test(Mockito.<RawDocument>any());
    }

    @Test
    public void complexFilterPath() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("field", "nested", "value"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(true);

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND p1 = ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
                  "field",
                  "nested",
                  "value",
                  "value",
                  "")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
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
      verify(filterExpression, times(1)).test(Mockito.<RawDocument>any());
    }

    @Test
    public void testNotPassed() throws Exception {
      int pageSize = 1;
      Paginator paginator = new Paginator(null, pageSize);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singleton("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.test(Mockito.<RawDocument>any())).thenReturn(false);
      when(filterExpression.getDescription()).thenReturn("field EQ something");

      ValidatingDataStore.QueryAssert queryAssert =
          withQuery(
                  TABLE,
                  "SELECT key, p0, p1, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM %s WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
                  "field",
                  "field",
                  "")
              .withPageSize(pageSize + 1)
              .returning(Collections.singletonList(ImmutableMap.of("key", "1")));

      DocumentsResolver resolver =
          new InMemoryDocumentsResolver(filterExpression, executionContext, configuration);
      Flowable<RawDocument> result =
          resolver.getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, paginator);

      result.test().await().assertValueCount(0).assertComplete();

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
