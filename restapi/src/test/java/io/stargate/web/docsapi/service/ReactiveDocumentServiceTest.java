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

package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Literal;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.awaitility.Awaitility;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReactiveDocumentServiceTest {

  ReactiveDocumentService reactiveDocumentService;

  ObjectMapper objectMapper = new ObjectMapper();

  @Mock ExpressionParser expressionParser;

  @Mock DocumentSearchService searchService;

  @Mock JsonConverter jsonConverter;

  @Mock TimeSource timeSource;

  @Mock DocumentDB documentDB;

  @Mock QueryExecutor queryExecutor;

  @Mock AuthorizationService authService;

  @Mock FilterExpression expression;

  @Mock FilterExpression expression2;

  @Mock FilterPath filterPath;

  @Mock FilterPath filterPath2;

  @Mock RawDocument rawDocument;

  @Mock Row row;

  @Mock ResultSet deleteResultSet;

  @Mock AuthenticationSubject authSubject;

  @BeforeEach
  public void init() {
    reactiveDocumentService =
        new ReactiveDocumentService(
            expressionParser, searchService, jsonConverter, objectMapper, timeSource);
    lenient()
        .when(documentDB.deleteDeadLeaves(any(), any(), any(), anyLong(), anyMap(), any()))
        .thenReturn(CompletableFuture.completedFuture(deleteResultSet));
    lenient().when(documentDB.authorizeDeleteDeadLeaves(any(), any())).thenReturn(true);
    lenient().when(documentDB.getAuthorizationService()).thenReturn(authService);
    lenient().when(documentDB.getAuthenticationSubject()).thenReturn(authSubject);
    lenient().when(expression.getFilterPath()).thenReturn(filterPath);
    lenient()
        .doAnswer(
            i -> {
              Set<FilterExpression> set = i.getArgument(0);
              set.add(expression);
              return null;
            })
        .when(expression)
        .collectK(any(), anyInt());
    lenient().when(row.columnExists(any())).thenReturn(true);
  }

  @Nested
  class FindDocuments {

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode();
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchDocuments(
              queryExecutor, namespace, collection, expression, paginator, context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Collections.singletonList(row)),
              eq(ImmutableDeadLeafCollector.of()),
              eq(false),
              anyBoolean());
      when(row.getString("p0")).thenReturn("myField");
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, where, fields, paginator, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isNull();
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().findValue(documentId)).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void happyPathFieldNotMatched() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode();
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchDocuments(
              queryExecutor, namespace, collection, expression, paginator, context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Collections.emptyList()),
              eq(ImmutableDeadLeafCollector.of()),
              eq(false),
              anyBoolean());
      when(row.getString("p0")).thenReturn("someField");
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, where, fields, paginator, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isNull();
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().findValue(documentId)).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void happyPathNoFields() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode();
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchDocuments(
              queryExecutor, namespace, collection, expression, paginator, context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Arrays.asList(row, row)),
              eq(ImmutableDeadLeafCollector.of()),
              eq(false),
              anyBoolean());
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Arrays.asList(row, row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, where, fields, paginator, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isNull();
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().findValue(documentId)).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void noResults() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchDocuments(
              queryExecutor, namespace, collection, expression, paginator, context))
          .thenReturn(Flowable.empty());

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, where, fields, paginator, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isNull();
                assertThat(wrapper.getData()).isNotNull();
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(wrapper.getPageState()).isNull();
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void noWhereNoFields() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      byte[] pageState = RandomUtils.nextBytes(64);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(searchService.searchDocuments(
              queryExecutor, namespace, collection, Literal.getTrue(), paginator, context))
          .thenReturn(docs);
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, null, null, paginator, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isNull();
                assertThat(wrapper.getData()).isNotNull();
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void notAuthorized() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, null, null, paginator, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verifyNoInteractions(searchService);
    }

    @Test
    public void whereJsonException() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = RandomStringUtils.randomAlphanumeric(16);

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, where, null, paginator, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_WHERE_JSON_INVALID);
                return true;
              });
      verifyNoInteractions(authService, searchService);
    }

    @Test
    public void fieldsJsonException() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = RandomStringUtils.randomAlphanumeric(16);

      Single<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findDocuments(
              documentDB, namespace, collection, null, fields, paginator, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
                return true;
              });
      verifyNoInteractions(authService, searchService);
    }
  }

  @Nested
  class GetDocument {

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode subDocumentNode = objectMapper.createObjectNode();
      ObjectNode documentNode = objectMapper.createObjectNode().set("prePath", subDocumentNode);
      ExecutionContext context = ExecutionContext.create(true);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = "[\"myField\"]";
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(searchService.getDocument(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Collections.singletonList("prePath"),
              context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(eq(Collections.singletonList(row)), any(), eq(false), anyBoolean());
      when(row.getString("p0")).thenReturn("prePath");
      when(row.getString("p1")).thenReturn("myField");
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.getDocument(
              documentDB, namespace, collection, documentId, prePath, fields, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
                assertThat(wrapper.getData()).isEqualTo(subDocumentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(wrapper.getPageState()).isNull();
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(documentDB, never()).deleteDeadLeaves(any(), any(), any(), anyLong(), anyMap(), any());
      verifyNoMoreInteractions(authService);
    }

    @Test
    public void happyPathWithDeadLeavesCollection() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode subDocumentNode = objectMapper.createObjectNode();
      ObjectNode documentNode = objectMapper.createObjectNode().set("prePath", subDocumentNode);
      ExecutionContext context = ExecutionContext.create(true);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = "[\"myField\"]";
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(searchService.getDocument(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Collections.singletonList("prePath"),
              context))
          .thenReturn(docs);

      when(row.getString("p0")).thenReturn("prePath");
      when(row.getString("p1")).thenReturn("myField");
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doAnswer(
              invocation -> {
                DeadLeafCollector collector = invocation.getArgument(1);
                collector.addAll("whatever");
                return documentNode;
              })
          .when(jsonConverter)
          .convertToJsonDoc(eq(rows), any(), eq(false), anyBoolean());

      Maybe<Pair<DocumentResponseWrapper<? extends JsonNode>, Disposable>> result =
          reactiveDocumentService.getDocumentInternal(
              documentDB, namespace, collection, documentId, prePath, fields, context);

      List<Pair<DocumentResponseWrapper<? extends JsonNode>, Disposable>> values =
          result.test().await().assertComplete().assertNoErrors().values();
      assertThat(values.size()).isEqualTo(1);

      DocumentResponseWrapper<? extends JsonNode> wrapper = values.get(0).getValue0();
      Disposable deleteBatch = values.get(0).getValue1();

      assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
      assertThat(wrapper.getData()).isEqualTo(subDocumentNode);
      assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
      assertThat(wrapper.getPageState()).isNull();

      Awaitility.await().atMost(Duration.ofSeconds(60)).until(deleteBatch::isDisposed);

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(documentDB)
          .deleteDeadLeaves(
              eq(namespace), eq(collection), eq(documentId), anyLong(), anyMap(), eq(context));
      verifyNoMoreInteractions(authService);
    }

    @Test
    public void unauthorizedWithDeadLeavesCollection() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode subDocumentNode = objectMapper.createObjectNode();
      ObjectNode documentNode = objectMapper.createObjectNode().set("prePath", subDocumentNode);
      ExecutionContext context = ExecutionContext.create(true);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = "[\"myField\"]";
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(searchService.getDocument(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Collections.singletonList("prePath"),
              context))
          .thenReturn(docs);

      when(row.getString("p0")).thenReturn("prePath");
      when(row.getString("p1")).thenReturn("myField");
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doAnswer(
              invocation -> {
                DeadLeafCollector collector = invocation.getArgument(1);
                collector.addAll("whatever");
                return documentNode;
              })
          .when(jsonConverter)
          .convertToJsonDoc(eq(rows), any(), eq(false), anyBoolean());

      when(documentDB.authorizeDeleteDeadLeaves(eq(namespace), eq(collection))).thenReturn(false);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.getDocument(
              documentDB, namespace, collection, documentId, prePath, fields, context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
                assertThat(wrapper.getData()).isEqualTo(subDocumentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(wrapper.getPageState()).isNull();
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(documentDB, never())
          .deleteDeadLeaves(
              eq(namespace), eq(collection), eq(documentId), anyLong(), anyMap(), eq(context));
      verifyNoMoreInteractions(authService);
    }
  }

  @Nested
  class FindSubDocuments {

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode().put("x", 1);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      when(filterPath.getField()).thenReturn("myField");
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchSubDocuments(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              expression,
              paginator,
              context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Collections.singletonList(row)),
              eq(ImmutableDeadLeafCollector.of()),
              eq(true),
              anyBoolean());
      when(row.getString("p0")).thenReturn("myField");
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              where,
              fields,
              paginator,
              context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().iterator().next()).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void happyPathWithPrePathOnly() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode().put("x", 1);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(searchService.searchSubDocuments(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Collections.singletonList("prePath"),
              Literal.getTrue(),
              paginator,
              context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Collections.singletonList(row)),
              eq(ImmutableDeadLeafCollector.of()),
              eq(true),
              anyBoolean());
      when(row.getString("p0")).thenReturn("prePath");
      when(row.getString("p1")).thenReturn("myField");
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              prePath,
              null,
              fields,
              paginator,
              context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().iterator().next()).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void happyPathWithPreAndParentPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode documentNode = objectMapper.createObjectNode().put("x", 1);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Flowable<RawDocument> docs = Flowable.just(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");
      when(filterPath.getField()).thenReturn("myField");
      when(filterPath.getParentPath()).thenReturn(Arrays.asList("prePath", "parentPath"));
      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
      when(expressionParser.constructFilterExpression(prePath, objectMapper.readTree(where), true))
          .thenReturn(expression);
      when(searchService.searchSubDocuments(
              queryExecutor,
              namespace,
              collection,
              documentId,
              Arrays.asList("prePath", "parentPath"),
              expression,
              paginator,
              context))
          .thenReturn(docs);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(
              eq(Collections.singletonList(row)),
              eq(ImmutableDeadLeafCollector.of()),
              eq(true),
              anyBoolean());
      when(row.getString("p0")).thenReturn("prePath");
      when(row.getString("p1")).thenReturn("parentPath");
      when(row.getString("p2")).thenReturn("myField");
      when(rawDocument.rows()).thenReturn(Collections.singletonList(row));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              prePath,
              where,
              fields,
              paginator,
              context);

      result
          .test()
          .await()
          .assertValue(
              wrapper -> {
                assertThat(wrapper.getDocumentId()).isEqualTo(documentId);
                assertThat(wrapper.getData()).hasSize(1);
                assertThat(wrapper.getData().iterator().next()).isEqualTo(documentNode);
                assertThat(wrapper.getProfile()).isEqualTo(context.toProfile());
                assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.getPageState()).array())
                    .isEqualTo(pageState);
                return true;
              })
          .assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
    }

    @Test
    public void notAuthenticated() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), false))
          .thenReturn(expression);
      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              where,
              null,
              paginator,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verifyNoInteractions(searchService);
    }

    @Test
    public void filedNotReferenced() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      String fields = "[\"myField\"]";
      when(filterPath.getField()).thenReturn("yourField");
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), false))
          .thenReturn(expression);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              where,
              fields,
              paginator,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_GET_CONDITION_FIELDS_NOT_REFERENCED);
                return true;
              });

      verifyNoInteractions(searchService, authService);
    }

    @Test
    public void multipleFilterPaths() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = "{}";
      when(expression2.getFilterPath()).thenReturn(filterPath2);
      doAnswer(
              i -> {
                Set<FilterExpression> set = i.getArgument(0);
                set.add(expression);
                set.add(expression2);
                return null;
              })
          .when(expression)
          .collectK(any(), anyInt());
      when(expressionParser.constructFilterExpression(
              Collections.emptyList(), objectMapper.readTree(where), false))
          .thenReturn(expression);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              where,
              null,
              paginator,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_GET_MULTIPLE_FIELD_CONDITIONS);
                return true;
              });

      verifyNoInteractions(searchService, authService);
    }

    @Test
    public void whereJsonException() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String where = RandomStringUtils.randomAlphanumeric(16);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              where,
              null,
              paginator,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_WHERE_JSON_INVALID);
                return true;
              });
      verifyNoInteractions(authService, searchService);
    }

    @Test
    public void fieldsJsonException() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String fields = RandomStringUtils.randomAlphanumeric(16);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.findSubDocuments(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              null,
              fields,
              paginator,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
                return true;
              });
      verifyNoInteractions(authService, searchService);
    }
  }
}
