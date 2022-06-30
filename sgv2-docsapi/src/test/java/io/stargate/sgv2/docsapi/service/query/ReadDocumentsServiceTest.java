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

package io.stargate.sgv2.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.OpenMocksTest;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.json.DeadLeaf;
import io.stargate.sgv2.docsapi.service.json.ImmutableDeadLeaf;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.util.ByteBufferUtils;
import io.stargate.sgv2.docsapi.service.write.WriteBridgeService;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class ReadDocumentsServiceTest {

  @Inject ReadDocumentsService service;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Inject DocumentProperties documentProperties;

  @InjectMock ReadBridgeService readBridgeService;

  @InjectMock WriteBridgeService writeBridgeService;

  Function<QueryOuterClass.Row, RowWrapper> wrapperFunction;

  QueryOuterClass.Row rowFor(String id, String value, String... path) {
    return rowFor(id, value, 0, path);
  }

  QueryOuterClass.Row rowFor(String id, String value, long writeTime, String... path) {
    String leaf = "";
    List<QueryOuterClass.Value> values = new ArrayList<>();
    values.add(Values.of(id));
    for (int i = 0; i < documentProperties.maxDepth(); i++) {
      if (i < path.length) {
        String p = path[i];
        leaf = p;
        values.add(Values.of(p));
      } else {
        values.add(Values.of(""));
      }
    }
    values.add(Values.of(leaf));
    values.add(Values.of(value));
    values.add(Values.NULL);
    values.add(Values.NULL);
    values.add(Values.of(writeTime));
    return QueryOuterClass.Row.newBuilder().addAllValues(values).build();
  }

  @BeforeEach
  public void initWrappingFunction() {
    List<QueryOuterClass.ColumnSpec> columns = new ArrayList<>(schemaProvider.allColumnSpec());
    columns.add(
        QueryOuterClass.ColumnSpec.newBuilder()
            .setName(documentProperties.tableProperties().writetimeColumnName())
            .build());
    wrapperFunction = RowWrapper.forColumns(columns);
  }

  @Nested
  class FindDocuments implements OpenMocksTest {

    @Mock RawDocument rawDocument;
    @Mock RawDocument rawDocument2;

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId1 = RandomStringUtils.randomAlphanumeric(16);
      String documentId2 = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState1 = RandomUtils.nextBytes(64);
      byte[] pageState2 = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument, rawDocument2);

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId1, "value1", "myField");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId1);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      QueryOuterClass.Row row2 = rowFor(documentId2, "value2", "myField");
      RowWrapper rowWrapper2 = wrapperFunction.apply(row2);
      when(rawDocument2.id()).thenReturn(documentId2);
      when(rawDocument2.rows()).thenReturn(Collections.singletonList(rowWrapper2));
      when(rawDocument2.makePagingState()).thenReturn(ByteBuffer.wrap(pageState2));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).hasSize(2);
      assertThat(wrapper.data().findValue(documentId1)).hasSize(1);
      assertThat(wrapper.data().findValue(documentId1).findValue("myField").textValue())
          .isEqualTo("value1");
      assertThat(wrapper.data().findValue(documentId2)).hasSize(1);
      assertThat(wrapper.data().findValue(documentId2).findValue("myField").textValue())
          .isEqualTo("value2");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState2);

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void happyPathFieldNotMatched() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      String where = "{}";
      String fields = "[\"myField\"]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().item(rawDocument);

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "otherField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue(documentId)).isEmpty();
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState);

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void happyPathNoFields() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      String where = "{}";
      String fields = "[]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().item(rawDocument);

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "otherField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue(documentId)).hasSize(1);
      assertThat(wrapper.data().findValue(documentId).findValue("otherField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState);

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void paginatorExhausted() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 2);

      String where = "{}";
      String fields = "[]";
      byte[] pageState = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().item(rawDocument);

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "otherField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue(documentId)).hasSize(1);
      assertThat(wrapper.data().findValue(documentId).findValue("otherField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void noResults() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      String where = "{}";
      String fields = "[]";
      Multi<RawDocument> docs = Multi.createFrom().empty();

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).isEmpty();
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void noWhereNoFields() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      byte[] pageState = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().item(rawDocument);

      when(readBridgeService.searchDocuments(
              eq(namespace), eq(collection), any(), eq(paginator), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "otherField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findDocuments(namespace, collection, null, null, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isNull();
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue(documentId)).hasSize(1);
      assertThat(wrapper.data().findValue(documentId).findValue("otherField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState);

      verify(readBridgeService)
          .searchDocuments(eq(namespace), eq(collection), any(), eq(paginator), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }
  }

  @Nested
  class GetDocument implements OpenMocksTest {

    @Mock RawDocument rawDocument;

    @Captor ArgumentCaptor<Map<String, Set<DeadLeaf>>> deadLeavesCaptor;

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      String fields = "[\"prePath\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);

      when(readBridgeService.getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "prePath", "myField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .getDocument(
                  namespace, collection, documentId, Collections.emptyList(), fields, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue("prePath")).hasSize(1);
      assertThat(wrapper.data().findValue("prePath").findValue("myField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();
    }

    @Test
    public void withPrePath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      String fields = "[\"myField\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);
      List<String> prePath = Collections.singletonList("prePath");

      when(readBridgeService.getDocument(
              eq(namespace), eq(collection), eq(documentId), eq(prePath), eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row = rowFor(documentId, "value", "prePath", "myField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .getDocument(namespace, collection, documentId, prePath, fields, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue("myField").textValue()).isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(readBridgeService)
          .getDocument(eq(namespace), eq(collection), eq(documentId), eq(prePath), eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void withDeadLeavesCollection() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);

      when(readBridgeService.getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context)))
          .thenReturn(docs);

      when(writeBridgeService.deleteDeadLeaves(
              eq(namespace), eq(collection), eq(documentId), anyLong(), any(), eq(context)))
          .thenReturn(Uni.createFrom().nothing());

      QueryOuterClass.Row oldRow = rowFor(documentId, "oldValue", 1L, "prePath");
      RowWrapper oldRowWrapper = wrapperFunction.apply(oldRow);
      QueryOuterClass.Row row = rowFor(documentId, "value", 2L, "prePath", "myField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(List.of(oldRowWrapper, rowWrapper));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .getDocument(
                  namespace, collection, documentId, Collections.emptyList(), null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue("prePath")).hasSize(1);
      assertThat(wrapper.data().findValue("prePath").findValue("myField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(writeBridgeService)
          .deleteDeadLeaves(
              eq(namespace),
              eq(collection),
              eq(documentId),
              anyLong(),
              deadLeavesCaptor.capture(),
              eq(context));
      assertThat(deadLeavesCaptor.getAllValues())
          .singleElement()
          .satisfies(
              deadLeaves ->
                  assertThat(deadLeaves)
                      .hasEntrySatisfying(
                          "$.prePath",
                          leafs ->
                              assertThat(leafs)
                                  .containsExactly(ImmutableDeadLeaf.builder().name("").build())));

      verify(readBridgeService)
          .getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void withDeadLeavesCollectionErrorIgnored() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);

      when(readBridgeService.getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context)))
          .thenReturn(docs);

      when(writeBridgeService.deleteDeadLeaves(
              eq(namespace), eq(collection), eq(documentId), anyLong(), any(), eq(context)))
          .thenReturn(Uni.createFrom().failure(new RuntimeException("This must be ignored!")));

      QueryOuterClass.Row oldRow = rowFor(documentId, "oldValue", 1L, "prePath");
      RowWrapper oldRowWrapper = wrapperFunction.apply(oldRow);
      QueryOuterClass.Row row = rowFor(documentId, "value", 2L, "prePath", "myField");
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(List.of(oldRowWrapper, rowWrapper));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .getDocument(
                  namespace, collection, documentId, Collections.emptyList(), null, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data()).hasSize(1);
      assertThat(wrapper.data().findValue("prePath")).hasSize(1);
      assertThat(wrapper.data().findValue("prePath").findValue("myField").textValue())
          .isEqualTo("value");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(readBridgeService)
          .getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context));
      verify(writeBridgeService)
          .deleteDeadLeaves(
              eq(namespace),
              eq(collection),
              eq(documentId),
              anyLong(),
              deadLeavesCaptor.capture(),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void notFound() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);

      String fields = "[\"prePath\"]";
      Multi<RawDocument> docs = Multi.createFrom().empty();

      when(readBridgeService.getDocument(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(Collections.emptyList()),
              eq(context)))
          .thenReturn(docs);

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .getDocument(
                  namespace, collection, documentId, Collections.emptyList(), fields, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper).isNull();
    }
  }

  @Nested
  class FindSubDocuments implements OpenMocksTest {

    @Mock RawDocument rawDocument;
    @Mock RawDocument rawDocument2;

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 2);

      byte[] pageState1 = RandomUtils.nextBytes(64);
      byte[] pageState2 = RandomUtils.nextBytes(64);
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument, rawDocument2);
      List<String> subPath = List.of("path");

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId, "value1", "path", "field1");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      QueryOuterClass.Row row2 = rowFor(documentId, "value2", "path", "field2");
      RowWrapper rowWrapper2 = wrapperFunction.apply(row2);
      when(rawDocument2.id()).thenReturn(documentId);
      when(rawDocument2.rows()).thenReturn(Collections.singletonList(rowWrapper2));
      when(rawDocument2.makePagingState()).thenReturn(ByteBuffer.wrap(pageState2));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, null, null, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data().isArray()).isTrue();
      assertThat(wrapper.data()).hasSize(2);
      assertThat(wrapper.data().get(0).findValue("path").findValue("field1").textValue())
          .isEqualTo("value1");
      assertThat(wrapper.data().get(1).findValue("path").findValue("field2").textValue())
          .isEqualTo("value2");
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState2);

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void withFieldsSelection() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 2);

      byte[] pageState1 = RandomUtils.nextBytes(64);
      byte[] pageState2 = RandomUtils.nextBytes(64);

      String fields = "[\"field1\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument, rawDocument2);
      List<String> subPath = List.of("path");

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId, "value1", "path", "field1");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      QueryOuterClass.Row row2 = rowFor(documentId, "value2", "path", "field2");
      RowWrapper rowWrapper2 = wrapperFunction.apply(row2);
      when(rawDocument2.id()).thenReturn(documentId);
      when(rawDocument2.rows()).thenReturn(Collections.singletonList(rowWrapper2));
      when(rawDocument2.makePagingState()).thenReturn(ByteBuffer.wrap(pageState2));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, null, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data().isArray()).isTrue();
      assertThat(wrapper.data())
          .singleElement()
          .satisfies(
              e ->
                  assertThat(e.findValue("path").findValue("field1").textValue())
                      .isEqualTo("value1"));
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState2);

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void withPreAndParentPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      byte[] pageState1 = RandomUtils.nextBytes(64);
      String where = "{\"parent.field1\": { \"$eq\": \"something\"}}";
      String fields = "[\"field1\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);
      List<String> subPath = List.of("path");

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(List.of("path", "parent")),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId, "value1", "path", "parent", "field1");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data().isArray()).isTrue();
      assertThat(wrapper.data())
          .singleElement()
          .satisfies(
              e ->
                  assertThat(
                          e.findValue("path").findValue("parent").findValue("field1").textValue())
                      .isEqualTo("value1"));
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState1);

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(List.of("path", "parent")),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void withoutPrePath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      byte[] pageState1 = RandomUtils.nextBytes(64);
      String where = "{\"parent.field1\": { \"$eq\": \"something\"}}";
      String fields = "[\"field1\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument);

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(List.of("parent")),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId, "value1", "parent", "field1");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace,
                  collection,
                  documentId,
                  Collections.emptyList(),
                  where,
                  fields,
                  paginator,
                  context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data().isArray()).isTrue();
      assertThat(wrapper.data())
          .singleElement()
          .satisfies(
              e ->
                  assertThat(e.findValue("parent").findValue("field1").textValue())
                      .isEqualTo("value1"));
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(ByteBufferUtils.fromBase64UrlParam(wrapper.pageState()).array())
          .isEqualTo(pageState1);

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(List.of("parent")),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void paginatorExhausted() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 3);

      byte[] pageState1 = RandomUtils.nextBytes(64);
      byte[] pageState2 = RandomUtils.nextBytes(64);
      String fields = "[\"field1\"]";
      Multi<RawDocument> docs = Multi.createFrom().items(rawDocument, rawDocument2);
      List<String> subPath = List.of("path");

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      QueryOuterClass.Row row1 = rowFor(documentId, "value1", "path", "field1");
      RowWrapper rowWrapper1 = wrapperFunction.apply(row1);
      when(rawDocument.id()).thenReturn(documentId);
      when(rawDocument.rows()).thenReturn(Collections.singletonList(rowWrapper1));
      when(rawDocument.makePagingState()).thenReturn(ByteBuffer.wrap(pageState1));

      QueryOuterClass.Row row2 = rowFor(documentId, "value2", "path", "field2");
      RowWrapper rowWrapper2 = wrapperFunction.apply(row2);
      when(rawDocument2.id()).thenReturn(documentId);
      when(rawDocument2.rows()).thenReturn(Collections.singletonList(rowWrapper2));
      when(rawDocument2.makePagingState()).thenReturn(ByteBuffer.wrap(pageState2));

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, null, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper.documentId()).isEqualTo(documentId);
      assertThat(wrapper.data().isArray()).isTrue();
      assertThat(wrapper.data())
          .singleElement()
          .satisfies(
              e ->
                  assertThat(e.findValue("path").findValue("field1").textValue())
                      .isEqualTo("value1"));
      assertThat(wrapper.profile()).isEqualTo(context.toProfile());
      assertThat(wrapper.pageState()).isNull();

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void noResults() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 1);

      Multi<RawDocument> docs = Multi.createFrom().empty();
      List<String> subPath = List.of("path");

      when(readBridgeService.searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context)))
          .thenReturn(docs);

      DocumentResponseWrapper<? extends JsonNode> wrapper =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, null, null, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(wrapper).isNull();

      verify(readBridgeService)
          .searchSubDocuments(
              eq(namespace),
              eq(collection),
              eq(documentId),
              eq(subPath),
              any(),
              eq(paginator),
              eq(context));
      verifyNoMoreInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void multipleFieldsNotAllowed() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 2);

      String where =
          "{\"otherField\": { \"$eq\": \"something\"}, \"otherField2\": { \"$eq\": \"something\"}}";
      List<String> subPath = List.of("path");

      Throwable failure =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, where, null, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GET_MULTIPLE_FIELD_CONDITIONS);

      verifyNoInteractions(readBridgeService, writeBridgeService);
    }

    @Test
    public void fieldNotReferenced() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      ExecutionContext context = ExecutionContext.create(true);
      Paginator paginator = new Paginator(null, 2);

      String where = "{\"otherField\": { \"$eq\": \"something\"}}";
      String fields = "[\"field1\"]";
      List<String> subPath = List.of("path");

      Throwable failure =
          service
              .findSubDocuments(
                  namespace, collection, documentId, subPath, where, fields, paginator, context)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GET_CONDITION_FIELDS_NOT_REFERENCED);

      verifyNoInteractions(readBridgeService, writeBridgeService);
    }
  }
}
