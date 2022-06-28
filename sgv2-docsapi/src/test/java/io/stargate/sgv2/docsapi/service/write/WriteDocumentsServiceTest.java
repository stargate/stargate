package io.stargate.sgv2.docsapi.service.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.model.dto.MultiDocsResponse;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonDocumentShredder;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.ReadBridgeService;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.model.paging.PagingStateSupplier;
import io.stargate.sgv2.docsapi.service.schema.JsonSchemaManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

@QuarkusTest
@TestProfile(WriteDocumentsServiceTest.Profile.class)
public class WriteDocumentsServiceTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.document.max-depth", "3")
          .put("stargate.document.max-array-length", "2")
          .build();
    }
  }

  @InjectMock WriteBridgeService writeBridgeService;

  @InjectMock ReadBridgeService readBridgeService;

  @InjectMock JsonDocumentShredder jsonDocumentShredder;

  @InjectMock JsonSchemaManager jsonSchemaManager;

  @InjectMock @Default TableManager tableManager;

  @Inject WriteDocumentsService documentWriteService;

  @Inject ObjectMapper objectMapper;

  @Mock List<JsonShreddedRow> rows;

  @Nested
  class WriteDocument {

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);

      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.writeDocument(
              eq(namespace), eq(collection), anyString(), eq(rows), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));
      when(jsonSchemaManager.validateJsonDocument(any(), any(), anyBoolean()))
          .thenReturn(Uni.createFrom().item(true));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .writeDocument(
                  Uni.createFrom().item(table), namespace, collection, obj, null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isNotNull();
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isNotNull();

      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentId(), rows, null, context);
      verifyNoMoreInteractions(writeBridgeService);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.writeDocument(
              eq(namespace), eq(collection), anyString(), eq(rows), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));
      when(jsonSchemaManager.validateJsonDocument(any(), any(), anyBoolean()))
          .thenReturn(Uni.createFrom().item(true));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .writeDocument(Uni.createFrom().item(table), namespace, collection, obj, 100, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();
      assertThat(result.documentId()).isNotNull();
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();

      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentId(), rows, 100, context);
      verifyNoMoreInteractions(writeBridgeService);
    }

    @Test
    public void happyPathWithSchemaCheck() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      Uni<Schema.CqlTable> tableUni = Uni.createFrom().item(table);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();
      JsonNode document = objectMapper.readTree(payload);
      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonDocumentShredder.shred(document, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.writeDocument(
              eq(namespace), eq(collection), anyString(), eq(rows), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .writeDocument(tableUni, namespace, collection, document, null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();
      assertThat(result.documentId()).isNotNull();
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentId(), rows, null, context);
      verify(jsonSchemaManager).validateJsonDocument(tableUni, document, false);
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }
  }

  @Nested
  class WriteDocuments {
    @Mock JsonShreddedRow jsonShreddedRow;

    List<JsonShreddedRow> rows1;

    List<JsonShreddedRow> rows2;

    @BeforeEach
    public void setupRows() {
      rows1 = new ArrayList<>();
      rows2 = new ArrayList<>();
      rows1.add(jsonShreddedRow);
    }

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeBridgeService.writeDocument(
              eq(namespace), eq(collection), anyString(), any(), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      MultiDocsResponse result =
          documentWriteService
              .writeDocuments(
                  Uni.createFrom().item(table), namespace, collection, obj, null, null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentIds().size()).isEqualTo(2);

      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentIds().get(0), rows1, null, context);
      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentIds().get(1), rows2, null, context);
      verify(jsonSchemaManager, times(2)).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeBridgeService.writeDocument(
              eq(namespace), eq(collection), anyString(), any(), eq(100), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      MultiDocsResponse result =
          documentWriteService
              .writeDocuments(
                  Uni.createFrom().item(table), namespace, collection, obj, null, 100, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentIds().size()).isEqualTo(2);

      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentIds().get(0), rows1, 100, context);
      verify(writeBridgeService)
          .writeDocument(namespace, collection, result.documentIds().get(1), rows2, 100, context);
      verify(jsonSchemaManager, times(2)).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithIdExtraction() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeBridgeService.updateDocument(
              eq(namespace), eq(collection), anyString(), any(), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      MultiDocsResponse result =
          documentWriteService
              .writeDocuments(
                  Uni.createFrom().item(table), namespace, collection, obj, "id", null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentIds().size()).isEqualTo(2);

      verify(writeBridgeService)
          .updateDocument(namespace, collection, result.documentIds().get(0), rows1, null, context);
      verify(writeBridgeService)
          .updateDocument(namespace, collection, result.documentIds().get(1), rows2, null, context);
      verify(jsonSchemaManager, times(2)).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void oneDocumentWriteFailed() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);
      when(writeBridgeService.updateDocument(
              eq(namespace), eq(collection), anyString(), eq(rows1), any(), eq(context)))
          .thenReturn(Uni.createFrom().failure(new IOException()));
      when(writeBridgeService.updateDocument(
              eq(namespace), eq(collection), anyString(), eq(rows2), any(), eq(context)))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      MultiDocsResponse result =
          documentWriteService
              .writeDocuments(
                  Uni.createFrom().item(table), namespace, collection, obj, "id", null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentIds()).isEqualTo(ImmutableList.of("2"));

      verify(writeBridgeService).updateDocument(namespace, collection, "1", rows1, null, context);
      verify(writeBridgeService)
          .updateDocument(namespace, collection, result.documentIds().get(0), rows2, null, context);
      verify(jsonSchemaManager, times(2)).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void schemaCheckFailed() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Uni<Schema.CqlTable> table = Uni.createFrom().item(Schema.CqlTable.newBuilder().build());
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"2\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      JsonNode schema = objectMapper.createObjectNode();

      ErrorCodeRuntimeException exception =
          new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonSchemaManager.validateJsonDocument(table, objectMapper.readTree(doc1Payload), false))
          .thenReturn(Uni.createFrom().item(true));
      when(jsonSchemaManager.validateJsonDocument(table, objectMapper.readTree(doc2Payload), false))
          .thenThrow(exception);

      documentWriteService
          .writeDocuments(table, namespace, collection, obj, "id", null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);

      verify(jsonSchemaManager)
          .validateJsonDocument(table, objectMapper.readTree(doc1Payload), false);
      verify(jsonSchemaManager)
          .validateJsonDocument(table, objectMapper.readTree(doc2Payload), false);
      verify(writeBridgeService, times(1))
          .updateDocument(any(), any(), anyString(), any(), any(), any());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void duplicateIds() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String doc1Payload = "{\"id\": \"1\"}";
      String doc2Payload = "{\"id\": \"1\"}";
      String payload = String.format("[%s,%s]", doc1Payload, doc2Payload);
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(objectMapper.readTree(doc1Payload), Collections.emptyList()))
          .thenReturn(rows1);
      when(jsonDocumentShredder.shred(objectMapper.readTree(doc2Payload), Collections.emptyList()))
          .thenReturn(rows2);

      assertThatThrownBy(
              () ->
                  documentWriteService.writeDocuments(
                      Uni.createFrom().item(table),
                      namespace,
                      collection,
                      obj,
                      "id",
                      null,
                      context))
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasMessage(
              "Found duplicate ID 1 in more than one document when doing batched document write.");
    }

    @Test
    public void notArrayPayload() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = String.format("{}");
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      assertThatThrownBy(
              () ->
                  documentWriteService.writeDocuments(
                      Uni.createFrom().item(table),
                      namespace,
                      collection,
                      obj,
                      "id",
                      null,
                      context))
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasMessage(ErrorCode.DOCS_API_WRITE_BATCH_NOT_ARRAY.getDefaultMessage());
    }
  }

  @Nested
  class UpdateDocument {
    RowWrapper row;
    RawDocument rawDocument;
    @Mock List<JsonShreddedRow> rows;

    @BeforeEach
    public void setup() {
      rawDocument =
          new RawDocument() {
            @Override
            public String id() {
              return null;
            }

            @Override
            public List<String> documentKeys() {
              return null;
            }

            @Override
            public PagingStateSupplier pagingState() {
              return null;
            }

            @Override
            public List<RowWrapper> rows() {
              return ImmutableList.of(row);
            }
          };
    }

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, null, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .updateDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  obj,
                  null,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, null, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, 100, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .updateDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  obj,
                  100,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, 100, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
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
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, subPath)).thenReturn(rows);
      when(writeBridgeService.updateDocument(
              namespace, collection, documentId, subPath, rows, 0, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .updateSubDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  subPath,
                  obj,
                  false,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .updateDocument(namespace, collection, documentId, subPath, rows, 0, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithSubPathTtlAuto() throws Exception {
      row =
          new RowWrapper() {
            @Override
            public List<QueryOuterClass.ColumnSpec> columns() {
              return null;
            }

            @Override
            public Map<String, Integer> columnIndexMap() {
              return null;
            }

            @Override
            public QueryOuterClass.Row row() {
              return null;
            }

            @Override
            public Long getLong(String columnName) {
              return 0L;
            }
          };

      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonDocumentShredder.shred(obj, subPath)).thenReturn(rows);
      when(writeBridgeService.updateDocument(
              namespace, collection, documentId, subPath, rows, 0, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));
      when(readBridgeService.getDocumentTtlInfo(any(), any(), any(), any()))
          .thenReturn(Uni.createFrom().item(rawDocument));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .updateSubDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  subPath,
                  obj,
                  true,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .updateDocument(namespace, collection, documentId, subPath, rows, 0, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
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
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonDocumentShredder.shred(document, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, null, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .updateDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  document,
                  null,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .updateDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, null, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void subDocumentWithSchemaCheck() throws JsonProcessingException {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      JsonNode schema = objectMapper.createObjectNode();
      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonSchemaManager.validateJsonDocument(any(), any(), anyBoolean())).thenCallRealMethod();

      documentWriteService
          .updateSubDocument(
              Uni.createFrom().item(table),
              namespace,
              collection,
              documentId,
              subPath,
              obj,
              false,
              context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);
    }

    @Test
    public void schemaCheckFailed() throws JsonProcessingException {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);
      JsonNode schema = objectMapper.createObjectNode();

      ErrorCodeRuntimeException exception =
          new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonSchemaManager.validateJsonDocument(any(), any(), anyBoolean())).thenThrow(exception);

      assertThatThrownBy(
              () ->
                  documentWriteService.updateDocument(
                      Uni.createFrom().item(table),
                      namespace,
                      collection,
                      documentId,
                      obj,
                      null,
                      context))
          .isInstanceOf(ErrorCodeRuntimeException.class);
    }
  }

  @Nested
  class PatchDocument {
    RowWrapper row;
    RawDocument rawDocument;
    @Mock List<JsonShreddedRow> rows;

    @BeforeEach
    public void setup() {
      rawDocument =
          new RawDocument() {
            @Override
            public String id() {
              return null;
            }

            @Override
            public List<String> documentKeys() {
              return null;
            }

            @Override
            public PagingStateSupplier pagingState() {
              return null;
            }

            @Override
            public List<RowWrapper> rows() {
              return ImmutableList.of(row);
            }
          };
    }

    @Test
    public void happyPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";
      JsonNode obj = objectMapper.readTree(payload);

      when(jsonDocumentShredder.shred(obj, Collections.emptyList())).thenReturn(rows);
      when(writeBridgeService.patchDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, 0, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .patchSubDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  Collections.emptyList(),
                  obj,
                  false,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .patchDocument(
              namespace, collection, documentId, Collections.emptyList(), rows, 0, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithSubPath() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";
      JsonNode obj = objectMapper.readTree(payload);

      when(jsonDocumentShredder.shred(obj, subPath)).thenReturn(rows);
      when(writeBridgeService.patchDocument(
              namespace, collection, documentId, subPath, rows, 0, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .patchSubDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  subPath,
                  obj,
                  false,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .patchDocument(namespace, collection, documentId, subPath, rows, 0, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void happyPathWithSubPathTtlAuto() throws Exception {
      row =
          new RowWrapper() {
            @Override
            public List<QueryOuterClass.ColumnSpec> columns() {
              return null;
            }

            @Override
            public Map<String, Integer> columnIndexMap() {
              return null;
            }

            @Override
            public QueryOuterClass.Row row() {
              return null;
            }

            @Override
            public Long getLong(String columnName) {
              return 0L;
            }
          };
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      String path = RandomStringUtils.randomAlphanumeric(16);
      List<String> subPath = Collections.singletonList(path);
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{\"key\":\"value\"}";
      JsonNode obj = objectMapper.readTree(payload);

      when(jsonDocumentShredder.shred(obj, subPath)).thenReturn(rows);
      when(writeBridgeService.patchDocument(
              namespace, collection, documentId, subPath, rows, 0, context))
          .thenReturn(Uni.createFrom().item(ResultSet.getDefaultInstance()));
      when(readBridgeService.getDocumentTtlInfo(any(), any(), any(), any()))
          .thenReturn(Uni.createFrom().item(rawDocument));

      DocumentResponseWrapper<Void> result =
          documentWriteService
              .patchSubDocument(
                  Uni.createFrom().item(table),
                  namespace,
                  collection,
                  documentId,
                  subPath,
                  obj,
                  true,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(result.documentId()).isEqualTo(documentId);
      assertThat(result.data()).isNull();
      assertThat(result.pageState()).isNull();
      assertThat(result.profile()).isEqualTo(context.toProfile());

      verify(writeBridgeService)
          .patchDocument(namespace, collection, documentId, subPath, rows, 0, context);
      verify(jsonSchemaManager).validateJsonDocument(any(), any(), anyBoolean());
      verifyNoMoreInteractions(writeBridgeService, jsonSchemaManager);
    }

    @Test
    public void withSchemaCheck() throws Exception {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode schema = objectMapper.createObjectNode();
      JsonNode obj = objectMapper.readTree(payload);

      when(jsonSchemaManager.getJsonSchema(any())).thenReturn(Uni.createFrom().item(schema));
      when(jsonSchemaManager.validateJsonDocument(any(), any(), anyBoolean())).thenCallRealMethod();

      documentWriteService
          .patchSubDocument(
              Uni.createFrom().item(table),
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              obj,
              false,
              context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);
    }

    @Test
    public void arrayNotAllowed() throws JsonProcessingException {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "[1]";
      JsonNode obj = objectMapper.readTree(payload);

      documentWriteService
          .patchSubDocument(
              Uni.createFrom().item(table),
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              obj,
              false,
              context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);
    }

    @Test
    public void emptyObjectNotAllowed() throws JsonProcessingException {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();
      ExecutionContext context = ExecutionContext.create(true);
      String payload = "{}";
      JsonNode obj = objectMapper.readTree(payload);

      documentWriteService
          .patchSubDocument(
              Uni.createFrom().item(table),
              namespace,
              collection,
              documentId,
              Collections.emptyList(),
              obj,
              false,
              context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);
    }
  }
}
