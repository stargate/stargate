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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Literal;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.core.util.TimeSource;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.MultiDocsResponse;
import io.stargate.web.docsapi.models.dto.ExecuteBuiltInFunction;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.write.DocumentWriteService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.awaitility.Awaitility;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReactiveDocumentServiceTest {

  ReactiveDocumentService reactiveDocumentService;

  ObjectMapper objectMapper = new ObjectMapper();

  @Mock DocsApiConfiguration config;

  @Mock ExpressionParser expressionParser;

  @Mock DocumentSearchService searchService;

  @Mock DocumentWriteService writeService;

  @Mock JsonConverter jsonConverter;

  @Mock JsonDocumentShredder jsonDocumentShredder;

  @Mock JsonSchemaHandler jsonSchemaHandler;

  @Mock TimeSource timeSource;

  @Mock DocumentDB documentDB;

  @Mock QueryExecutor queryExecutor;

  @Mock DataStore dataStore;

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
            expressionParser,
            searchService,
            writeService,
            jsonConverter,
            jsonSchemaHandler,
            jsonDocumentShredder,
            objectMapper,
            timeSource,
            config);
    lenient().when(config.getMaxArrayLength()).thenReturn(99999);
    lenient()
        .when(documentDB.deleteDeadLeaves(any(), any(), any(), anyLong(), anyMap(), any()))
        .thenReturn(CompletableFuture.completedFuture(deleteResultSet));
    lenient().when(documentDB.authorizeDeleteDeadLeaves(any(), any())).thenReturn(true);
    lenient().when(documentDB.getAuthorizationService()).thenReturn(authService);
    lenient().when(documentDB.getAuthenticationSubject()).thenReturn(authSubject);
    lenient().when(documentDB.getQueryExecutor()).thenReturn(queryExecutor);
    lenient().when(queryExecutor.getDataStore()).thenReturn(dataStore);
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
  class WriteDocument {

    @Mock List<JsonShreddedRow> rows;

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), Collections.emptyList()))
          .thenReturn(rows);
      when(writeService.writeDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              eq(rows),
              any(),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, null, context);

      MutableObject<String> documentId = new MutableObject<>();
      result
          .test()
          .await()
          .assertValue(
              v -> {
                documentId.setValue(v.getDocumentId());
                assertThat(v.getDocumentId()).isNotNull();
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId.getValue(), rows, null, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), Collections.emptyList()))
          .thenReturn(rows);
      when(writeService.writeDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              eq(rows),
              eq(100),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, 100, context);

      MutableObject<String> documentId = new MutableObject<>();
      result
          .test()
          .await()
          .assertValue(
              v -> {
                documentId.setValue(v.getDocumentId());
                assertThat(v.getDocumentId()).isNotNull();
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId.getValue(), rows, 100, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSchemaCheck() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();
      JsonNode document = objectMapper.readTree(payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(false);
      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);
      when(jsonDocumentShredder.shred(document, Collections.emptyList())).thenReturn(rows);
      when(writeService.writeDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              eq(rows),
              any(),
              eq(false),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, null, context);

      MutableObject<String> documentId = new MutableObject<>();
      result
          .test()
          .await()
          .assertValue(
              v -> {
                documentId.setValue(v.getDocumentId());
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId.getValue(), rows, null, false, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verify(jsonSchemaHandler).validate(schema, document);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void malformedJson() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":}";

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_INVALID_JSON_VALUE);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void schemaCheckFailed() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      ObjectNode schema = objectMapper.createObjectNode();

      ErrorCodeRuntimeException exception =
          new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);
      doThrow(exception).when(jsonSchemaHandler).validate(schema, objectMapper.readTree(payload));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, null, context);

      result.test().await().assertError(exception);

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void unauthorized() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.writeDocument(
              documentDB, namespace, collection, payload, null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }
  }

  @Nested
  class WriteDocuments {

    @Mock List<JsonShreddedRow> rows1;

    @Mock List<JsonShreddedRow> rows2;

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeService.writeDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              any(),
              any(),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, null, null, context);

      MutableObject<String> documentId1 = new MutableObject<>();
      MutableObject<String> documentId2 = new MutableObject<>();
      result
          .test()
          .await()
          .assertValue(
              v -> {
                List<String> documentIds = v.getDocumentIds();
                assertThat(documentIds).hasSize(2).allSatisfy(id -> assertThat(id).isNotEmpty());
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                documentId1.setValue(documentIds.get(0));
                documentId2.setValue(documentIds.get(1));
                return true;
              });

      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId1.getValue(), rows1, null, true, context);
      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId2.getValue(), rows2, null, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeService.writeDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              any(),
              eq(100),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, null, 100, context);

      MutableObject<String> documentId1 = new MutableObject<>();
      MutableObject<String> documentId2 = new MutableObject<>();
      result
          .test()
          .await()
          .assertValue(
              v -> {
                List<String> documentIds = v.getDocumentIds();
                assertThat(documentIds).hasSize(2).allSatisfy(id -> assertThat(id).isNotEmpty());
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                documentId1.setValue(documentIds.get(0));
                documentId2.setValue(documentIds.get(1));
                return true;
              });

      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId1.getValue(), rows1, 100, true, context);
      verify(writeService)
          .writeDocument(
              dataStore, namespace, collection, documentId2.getValue(), rows2, 100, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithIdExtraction() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeService.updateDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              any(),
              any(),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                List<String> documentIds = v.getDocumentIds();
                assertThat(documentIds).hasSize(2).containsExactly("1", "2");
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(dataStore, namespace, collection, "1", rows1, null, true, context);
      verify(writeService)
          .updateDocument(dataStore, namespace, collection, "2", rows2, null, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void oneDocumentWriteFailed() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeService.updateDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              eq(rows1),
              any(),
              eq(true),
              eq(context)))
          .thenReturn(Single.error(new IOException()));
      when(writeService.updateDocument(
              eq(dataStore),
              eq(namespace),
              eq(collection),
              anyString(),
              eq(rows2),
              any(),
              eq(true),
              eq(context)))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                List<String> documentIds = v.getDocumentIds();
                assertThat(documentIds).hasSize(1).containsExactly("2");
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(dataStore, namespace, collection, "1", rows1, null, true, context);
      verify(writeService)
          .updateDocument(dataStore, namespace, collection, "2", rows2, null, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void schemaCheckFailed() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      ObjectNode schema = objectMapper.createObjectNode();

      ErrorCodeRuntimeException exception =
          new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);
      doNothing().when(jsonSchemaHandler).validate(schema, objectMapper.readTree(doc1Payload));
      doThrow(exception)
          .when(jsonSchemaHandler)
          .validate(schema, objectMapper.readTree(doc2Payload));

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isEqualTo(exception);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verify(jsonSchemaHandler).validate(schema, objectMapper.readTree(doc1Payload));
      verify(jsonSchemaHandler).validate(schema, objectMapper.readTree(doc2Payload));
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void duplicateIds() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"1\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_WRITE_BATCH_DUPLICATE_ID);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler, times(2)).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void notArrayPayload() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = String.format("{}");

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_WRITE_BATCH_NOT_ARRAY);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void unauthorized() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"1\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);

      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);

      Single<MultiDocsResponse> result =
          reactiveDocumentService.writeDocuments(
              documentDB, namespace, collection, payload, "id", null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }
  }

  @Nested
  class UpdateDocument {

    @Mock List<JsonShreddedRow> rows;

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), Collections.emptyList()))
          .thenReturn(rows);
      when(writeService.updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              null,
              true,
              context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, null, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              null,
              true,
              context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), Collections.emptyList()))
          .thenReturn(rows);
      when(writeService.updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              100,
              true,
              context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, 100, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              100,
              true,
              context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSubPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), subPath)).thenReturn(rows);
      when(writeService.updateDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateSubDocument(
              documentDB, namespace, collection, documentId, subPath, payload, false, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSubPathTtlAuto() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), subPath)).thenReturn(rows);
      when(writeService.updateDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context))
          .thenReturn(Single.just(ResultSet.empty()));
      when(rawDocument.rows()).thenReturn(ImmutableList.of(row));
      when(row.getInt("ttl(leaf)")).thenReturn(0);
      when(searchService.getDocumentTtlInfo(any(), any(), any(), any(), any()))
          .thenReturn(Flowable.just(rawDocument));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateSubDocument(
              documentDB, namespace, collection, documentId, subPath, payload, true, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSchemaCheck() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();
      JsonNode document = objectMapper.readTree(payload);

      when(documentDB.treatBooleansAsNumeric()).thenReturn(false);
      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);
      when(jsonDocumentShredder.shred(document, Collections.emptyList())).thenReturn(rows);
      when(writeService.updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              null,
              false,
              context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, null, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .updateDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              null,
              false,
              context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verify(jsonSchemaHandler).validate(schema, document);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void subDocumentWithSchemaCheck() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();

      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateSubDocument(
              documentDB, namespace, collection, documentId, subPath, payload, false, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_JSON_SCHEMA_INVALID_PARTIAL_UPDATE);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void malformedJson() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":}";

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_INVALID_JSON_VALUE);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void schemaCheckFailed() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      ObjectNode schema = objectMapper.createObjectNode();

      ErrorCodeRuntimeException exception =
          new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);
      doThrow(exception).when(jsonSchemaHandler).validate(schema, objectMapper.readTree(payload));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, null, context);

      result.test().await().assertError(exception);

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void unauthorized() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      doNothing()
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.updateDocument(
              documentDB, namespace, collection, documentId, payload, null, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }
  }

  @Nested
  class PatchDocument {

    @Mock List<JsonShreddedRow> rows;

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), Collections.emptyList()))
          .thenReturn(rows);
      when(writeService.patchDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              0,
              true,
              context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .patchDocument(
              dataStore,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              rows,
              0,
              true,
              context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSubPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), subPath)).thenReturn(rows);
      when(writeService.patchDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB, namespace, collection, documentId, subPath, payload, false, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .patchDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void happyPathWithSubPathTtlAuto() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";

      when(documentDB.treatBooleansAsNumeric()).thenReturn(true);
      when(jsonDocumentShredder.shred(objectMapper.readTree(payload), subPath)).thenReturn(rows);
      when(writeService.patchDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context))
          .thenReturn(Single.just(ResultSet.empty()));
      when(rawDocument.rows()).thenReturn(ImmutableList.of(row));
      when(row.getInt("ttl(leaf)")).thenReturn(0);
      when(searchService.getDocumentTtlInfo(any(), any(), any(), any(), any()))
          .thenReturn(Flowable.just(rawDocument));

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB, namespace, collection, documentId, subPath, payload, true, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getDocumentId()).isEqualTo(documentId);
                assertThat(v.getData()).isNull();
                assertThat(v.getPageState()).isNull();
                assertThat(v.getProfile()).isEqualTo(context.toProfile());
                return true;
              });

      verify(writeService)
          .patchDocument(
              dataStore, namespace, collection, documentId, subPath, rows, 0, true, context);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void withSchemaCheck() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();

      when(jsonSchemaHandler.getCachedJsonSchema(documentDB, namespace, collection))
          .thenReturn(schema);

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_JSON_SCHEMA_INVALID_PARTIAL_UPDATE);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void arrayNotAllowed() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "[1]";

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void emptyObjectNotAllowed() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_PATCH_EMPTY_NOT_ACCEPTED);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonSchemaHandler).getCachedJsonSchema(documentDB, namespace, collection);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void malformedJson() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":}";

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_INVALID_JSON_VALUE);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }

    @Test
    public void unauthorized() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";

      doNothing()
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);

      Single<DocumentResponseWrapper<Void>> result =
          reactiveDocumentService.patchDocument(
              documentDB,
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              payload,
              false,
              context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(writeService, authService, searchService, jsonSchemaHandler);
    }
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

  @Nested
  class DoExecuteBuiltInFunction {

    @Mock List<JsonShreddedRow> shreddedRows;

    @Captor ArgumentCaptor<JsonNode> addedNode;

    @Test
    public void pushHappyPath() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$push", value);

      ObjectNode documentNode = objectMapper.createObjectNode();
      documentNode.set("path", objectMapper.createArrayNode());
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));
      when(searchService.getDocumentTtlInfo(queryExecutor, namespace, collection, docId, context))
          .thenReturn(Flowable.just(rawDocument));
      when(writeService.updateDocument(
              dataStore, namespace, collection, docId, path, shreddedRows, 0, false, context))
          .thenReturn(Single.just(ResultSet.empty()));
      when(jsonDocumentShredder.shred(Mockito.<JsonNode>any(), anyList())).thenReturn(shreddedRows);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getData())
                    .isInstanceOf(ArrayNode.class)
                    .hasSize(1)
                    .allSatisfy(
                        inner -> {
                          assertThat(inner.isTextual()).isTrue();
                          assertThat(inner.textValue()).isEqualTo(value);
                        });
                return true;
              })
          .assertComplete();

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder).shred(addedNode.capture(), eq(path));
      assertThat(addedNode.getAllValues())
          .allSatisfy(
              node -> {
                assertThat(node)
                    .isInstanceOf(ArrayNode.class)
                    .hasSize(1)
                    .allSatisfy(
                        inner -> {
                          assertThat(inner.isTextual()).isTrue();
                          assertThat(inner.textValue()).isEqualTo(value);
                        });
              });
    }

    @Test
    public void pushNotArray() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$push", value);

      ObjectNode documentNode = objectMapper.createObjectNode();
      documentNode.set("path", objectMapper.createObjectNode());
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID);
                return true;
              });

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder, never()).shred(Mockito.<JsonNode>any(), anyList());
      verify(writeService, never())
          .updateDocument(any(), any(), any(), any(), anyList(), any(), anyBoolean(), any());
    }

    @Test
    public void pushMissingNode() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$push", value);

      ObjectNode documentNode = objectMapper.createObjectNode();
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result.test().await().assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder, never()).shred(Mockito.<JsonNode>any(), anyList());
      verify(writeService, never())
          .updateDocument(any(), any(), any(), any(), anyList(), any(), anyBoolean(), any());
    }

    @Test
    public void popHappyPath() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$pop", null);

      ObjectNode documentNode = objectMapper.createObjectNode();
      ArrayNode arrayNode = objectMapper.createArrayNode().add(value);
      documentNode.set("path", arrayNode);
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));
      when(searchService.getDocumentTtlInfo(queryExecutor, namespace, collection, docId, context))
          .thenReturn(Flowable.just(rawDocument));
      when(writeService.updateDocument(
              dataStore, namespace, collection, docId, path, shreddedRows, 0, false, context))
          .thenReturn(Single.just(ResultSet.empty()));
      when(jsonDocumentShredder.shred(Mockito.<JsonNode>any(), anyList())).thenReturn(shreddedRows);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result
          .test()
          .await()
          .assertValue(
              v -> {
                assertThat(v.getData().isTextual()).isTrue();
                assertThat(v.getData().textValue()).isEqualTo(value);
                return true;
              })
          .assertComplete();

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder).shred(addedNode.capture(), eq(path));
      assertThat(addedNode.getAllValues())
          .allSatisfy(
              node -> {
                assertThat(node).isInstanceOf(ArrayNode.class).hasSize(0);
              });
    }

    @Test
    public void popNotArray() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$pop", null);

      ObjectNode documentNode = objectMapper.createObjectNode();
      documentNode.set("path", objectMapper.createObjectNode());
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID);
                return true;
              });

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder, never()).shred(Mockito.<JsonNode>any(), anyList());
      verify(writeService, never())
          .updateDocument(any(), any(), any(), any(), anyList(), any(), anyBoolean(), any());
    }

    @Test
    public void popMissingNode() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$pop", null);

      ObjectNode documentNode = objectMapper.createObjectNode();
      List<Row> rows = Collections.singletonList(row);
      when(rawDocument.rows()).thenReturn(rows);
      doReturn(documentNode)
          .when(jsonConverter)
          .convertToJsonDoc(any(), any(), anyBoolean(), anyBoolean());
      when(searchService.getDocument(queryExecutor, namespace, collection, docId, path, context))
          .thenReturn(Flowable.just(rawDocument));

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result.test().await().assertComplete();

      verify(authService).authorizeDataRead(authSubject, namespace, collection, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.MODIFY, SourceAPI.REST);
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(jsonDocumentShredder, never()).shred(Mockito.<JsonNode>any(), anyList());
      verify(writeService, never())
          .updateDocument(any(), any(), any(), any(), anyList(), any(), anyBoolean(), any());
    }

    @Test
    public void unknownFunction() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      List<String> path = Collections.singletonList("path");
      ExecuteBuiltInFunction function = new ExecuteBuiltInFunction("$my-function", null);

      Maybe<DocumentResponseWrapper<? extends JsonNode>> result =
          reactiveDocumentService.executeBuiltInFunction(
              documentDB, namespace, collection, docId, function, path, context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e)
                    .isInstanceOf(ErrorCodeRuntimeException.class)
                    .hasFieldOrPropertyWithValue(
                        "errorCode", ErrorCode.DOCS_API_INVALID_BUILTIN_FUNCTION);
                return true;
              });

      verifyNoInteractions(writeService, searchService);
    }
  }

  @Nested
  class DeleteDocument {

    @Test
    public void happyPath() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      when(writeService.deleteDocument(
              dataStore, namespace, collection, docId, Collections.emptyList(), context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<Boolean> result =
          reactiveDocumentService.deleteDocument(
              documentDB, namespace, collection, docId, Collections.emptyList(), context);

      result.test().await().assertValue(success -> success).assertComplete();

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(writeService)
          .deleteDocument(
              dataStore, namespace, collection, docId, Collections.emptyList(), context);
      verifyNoMoreInteractions(authService, writeService, searchService, jsonDocumentShredder);
    }

    @Test
    public void happyPathSubDocument() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Arrays.asList("transactions", "[0]");

      when(writeService.deleteDocument(
              dataStore,
              namespace,
              collection,
              docId,
              Arrays.asList("transactions", "[000000]"),
              context))
          .thenReturn(Single.just(ResultSet.empty()));

      Single<Boolean> result =
          reactiveDocumentService.deleteDocument(
              documentDB, namespace, collection, docId, subPath, context);

      result.test().await().assertValue(success -> success).assertComplete();

      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verify(writeService)
          .deleteDocument(
              dataStore,
              namespace,
              collection,
              docId,
              Arrays.asList("transactions", "[000000]"),
              context);
      verifyNoMoreInteractions(authService, writeService, searchService, jsonDocumentShredder);
    }

    @Test
    public void unauthorized() throws Exception {
      ExecutionContext context = ExecutionContext.create(true);
      String docId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      doThrow(UnauthorizedException.class)
          .when(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);

      Single<Boolean> result =
          reactiveDocumentService.deleteDocument(
              documentDB, namespace, collection, docId, Collections.emptyList(), context);

      result
          .test()
          .await()
          .assertError(
              e -> {
                assertThat(e).isInstanceOf(UnauthorizedException.class);
                return true;
              });
      verify(authService)
          .authorizeDataWrite(authSubject, namespace, collection, Scope.DELETE, SourceAPI.REST);
      verifyNoMoreInteractions(authService, writeService, searchService, jsonDocumentShredder);
    }
  }
}
