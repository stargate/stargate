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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnyFiltersResolverTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(0);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Mock DocsApiConfiguration configuration;

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
  }

  @Nested
  class GetDocuments {

    @Mock CandidatesFilter candidatesFilter;

    @Mock CandidatesFilter candidatesFilter2;

    @Mock RawDocument rawDocument;

    @Mock RawDocument rawDocument2;

    Single<? extends Query<? extends BoundQuery>> query1;

    Single<? extends Query<? extends BoundQuery>> query2;

    @BeforeEach
    public void initQueries() {
      DataStore datastore = datastore();

      query1 =
          RxUtils.singleFromFuture(
                  () -> {
                    BuiltQuery<? extends BoundQuery> built =
                        datastore.queryBuilder().select().column("text_value").from(TABLE).build();
                    return datastore.prepare(built);
                  })
              .cache();

      query2 =
          RxUtils.singleFromFuture(
                  () -> {
                    BuiltQuery<? extends BoundQuery> built =
                        datastore.queryBuilder().select().column("dbl_value").from(TABLE).build();
                    return datastore.prepare(built);
                  })
              .cache();
    }

    @Test
    public void happyPath() throws Exception {
      withAnySelectFrom(TABLE).returningNothing();

      DataStore datastore = datastore();
      doAnswer(i -> query1)
          .when(candidatesFilter)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2)
          .when(candidatesFilter2)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Maybe.just("you shall pass"))
          .when(candidatesFilter)
          .bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument);
      doAnswer(i -> Maybe.just("you shall pass"))
          .when(candidatesFilter2)
          .bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument);
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) -> Flowable.just(rawDocument);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      Flowable<RawDocument> results =
          resolver.getDocuments(
              queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1));

      results.test().await().assertValue(rawDocument).assertComplete();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void multipleDocumentsOneFilterPassing() throws Exception {
      withAnySelectFrom(TABLE).returningNothing();

      DataStore datastore = datastore();
      doAnswer(i -> query1)
          .when(candidatesFilter)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2)
          .when(candidatesFilter2)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Maybe.empty())
          .when(candidatesFilter)
          .bindAndFilter(eq(queryExecutor), eq(query1.blockingGet()), any());
      doAnswer(i -> Maybe.just("you shall pass"))
          .when(candidatesFilter2)
          .bindAndFilter(eq(queryExecutor), eq(query2.blockingGet()), any());
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) ->
              Flowable.just(rawDocument, rawDocument2);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      Flowable<RawDocument> results =
          resolver.getDocuments(
              queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1));

      results
          .test()
          .await()
          .assertValueAt(0, rawDocument)
          .assertValueAt(1, rawDocument2)
          .assertComplete();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument2);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument2);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void allFiltersNotPassed() throws Exception {
      withAnySelectFrom(TABLE).returningNothing();

      DataStore datastore = datastore();
      doAnswer(i -> query1)
          .when(candidatesFilter)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2)
          .when(candidatesFilter2)
          .prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Maybe.empty())
          .when(candidatesFilter)
          .bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument);
      doAnswer(i -> Maybe.empty())
          .when(candidatesFilter2)
          .bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument);
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) -> Flowable.just(rawDocument);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      Flowable<RawDocument> results =
          resolver.getDocuments(
              queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1));

      results.test().await().assertValueCount(0).assertComplete();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(datastore, KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1.blockingGet(), rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2.blockingGet(), rawDocument);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void noCandidates() throws Exception {
      withAnySelectFrom(TABLE).returningNothing();

      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) -> Flowable.empty();

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      Flowable<RawDocument> results =
          resolver.getDocuments(
              queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1));

      results.test().await().assertValueCount(0).assertComplete();

      resetExpectations();
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }
  }
}
