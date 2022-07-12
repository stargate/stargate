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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.docsapi.OpenMocksTest;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Arrays;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

@QuarkusTest
class AnyFiltersResolverTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject QueryExecutor queryExecutor;

  ExecutionContext executionContext;

  @BeforeEach
  public void init() {
    executionContext = ExecutionContext.create(true);
  }

  @Nested
  class GetDocuments implements OpenMocksTest {

    @Mock CandidatesFilter candidatesFilter;

    @Mock CandidatesFilter candidatesFilter2;

    @Mock RawDocument rawDocument;

    @Mock RawDocument rawDocument2;

    Uni<QueryOuterClass.Query> query1;

    Uni<QueryOuterClass.Query> query2;

    @BeforeEach
    public void initQueries() {
      query1 =
          Uni.createFrom()
              .item(
                  () ->
                      new QueryBuilder()
                          .select()
                          .column("text_value")
                          .from(COLLECTION_NAME)
                          .build())
              .memoize()
              .indefinitely();

      query2 =
          Uni.createFrom()
              .item(
                  () ->
                      new QueryBuilder().select().column("dbl_value").from(COLLECTION_NAME).build())
              .memoize()
              .indefinitely();
    }

    @Test
    public void happyPath() {
      withAnySelectFrom(KEYSPACE_NAME, COLLECTION_NAME).returningNothing();
      QueryOuterClass.Query query1Final = query1.await().indefinitely();
      QueryOuterClass.Query query2Final = query2.await().indefinitely();

      doAnswer(i -> query1).when(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2).when(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Uni.createFrom().item(true))
          .when(candidatesFilter)
          .bindAndFilter(queryExecutor, query1Final, rawDocument);
      doAnswer(i -> Uni.createFrom().item(true))
          .when(candidatesFilter2)
          .bindAndFilter(queryExecutor, query2Final, rawDocument);
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) ->
              Multi.createFrom().items(rawDocument);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1))
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitItems(1)
          .assertLastItem(rawDocument)
          .awaitCompletion()
          .assertCompleted();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1Final, rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2Final, rawDocument);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void multipleDocumentsOneFilterPassing() {
      withAnySelectFrom(KEYSPACE_NAME, COLLECTION_NAME).returningNothing();
      QueryOuterClass.Query query1Final = query1.await().indefinitely();
      QueryOuterClass.Query query2Final = query2.await().indefinitely();

      doAnswer(i -> query1).when(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2).when(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Uni.createFrom().item(false))
          .when(candidatesFilter)
          .bindAndFilter(eq(queryExecutor), eq(query1Final), any());
      doAnswer(i -> Uni.createFrom().item(true))
          .when(candidatesFilter2)
          .bindAndFilter(eq(queryExecutor), eq(query2Final), any());
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) ->
              Multi.createFrom().items(rawDocument, rawDocument2);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1))
          .subscribe()
          .withSubscriber(AssertSubscriber.create(2))
          .awaitItems(2)
          .assertItems(rawDocument, rawDocument2)
          .awaitCompletion()
          .assertCompleted();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1Final, rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2Final, rawDocument);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1Final, rawDocument2);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2Final, rawDocument2);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void allFiltersNotPassed() {
      withAnySelectFrom(KEYSPACE_NAME, COLLECTION_NAME).returningNothing();
      QueryOuterClass.Query query1Final = query1.await().indefinitely();
      QueryOuterClass.Query query2Final = query2.await().indefinitely();

      doAnswer(i -> query1).when(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> query2).when(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      doAnswer(i -> Uni.createFrom().item(false))
          .when(candidatesFilter)
          .bindAndFilter(queryExecutor, query1Final, rawDocument);
      doAnswer(i -> Uni.createFrom().item(false))
          .when(candidatesFilter2)
          .bindAndFilter(queryExecutor, query2Final, rawDocument);
      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) ->
              Multi.createFrom().items(rawDocument);

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1))
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      resetExpectations();

      verify(candidatesFilter).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter2).prepareQuery(KEYSPACE_NAME, COLLECTION_NAME);
      verify(candidatesFilter).bindAndFilter(queryExecutor, query1Final, rawDocument);
      verify(candidatesFilter2).bindAndFilter(queryExecutor, query2Final, rawDocument);
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }

    @Test
    public void noCandidates() {
      withAnySelectFrom(KEYSPACE_NAME, COLLECTION_NAME).returningNothing();

      DocumentsResolver candidatesResolver =
          (queryExecutor1, keyspace, collection, paginator) -> Multi.createFrom().empty();

      DocumentsResolver resolver =
          new AnyFiltersResolver(
              Arrays.asList((c) -> candidatesFilter, (c) -> candidatesFilter2),
              executionContext,
              candidatesResolver);
      resolver
          .getDocuments(queryExecutor, KEYSPACE_NAME, COLLECTION_NAME, new Paginator(null, 1))
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      resetExpectations();
      verifyNoMoreInteractions(candidatesFilter, candidatesFilter2);
    }
  }
}
