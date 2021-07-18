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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Literal;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
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

  @Mock RawDocument rawDocument;

  @Mock Row row;

  @Mock AuthenticationSubject authSubject;

  @BeforeEach
  public void init() {
    reactiveDocumentService =
        new ReactiveDocumentService(
            expressionParser, searchService, jsonConverter, objectMapper, timeSource);
    lenient().when(documentDB.getAuthorizationService()).thenReturn(authService);
    lenient().when(documentDB.getAuthenticationSubject()).thenReturn(authSubject);
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
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verifyNoInteractions(searchService);
    }

    @Test
    public void whereJsonException() {
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
    public void fieldsJsonException() {
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
