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
package io.stargate.sgv2.docsapi.service.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.json.DeadLeaf;
import io.stargate.sgv2.docsapi.service.json.ImmutableDeadLeaf;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class WriteBridgeServiceTest extends AbstractValidatingStargateBridgeTest {

  WriteBridgeService service;
  @Inject DocsApiTestSchemaProvider schemaProvider;
  @Inject DataStoreProperties dataStoreProperties;
  @Inject DocumentProperties documentProperties;
  @Inject QueriesConfig queriesConfig;
  @InjectMock TimeSource timeSource;

  String keyspaceName;
  String tableName;
  Batch.Type expectedBatchType;
  String documentId;
  long timestamp;
  ExecutionContext context;

  @BeforeEach
  public void init() {
    service =
        new WriteBridgeService(
            bridge, timeSource, dataStoreProperties, documentProperties, queriesConfig);
    keyspaceName = schemaProvider.getKeyspace().getName();
    tableName = schemaProvider.getTable().getName();
    expectedBatchType =
        dataStoreProperties.loggedBatchesEnabled() ? Batch.Type.LOGGED : Batch.Type.UNLOGGED;
    documentId = RandomStringUtils.randomAlphanumeric(16);
    timestamp = RandomUtils.nextLong();
    when(timeSource.currentTimeMicros()).thenReturn(timestamp);
    context = ExecutionContext.create(true);
  }

  @Nested
  class WriteDocument {

    @Test
    public void happyPath() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s "
                  + "(key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .writeDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);

      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC INSERT");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(insertCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      int ttl = 100;

      String insertCql =
          String.format(
              "INSERT INTO %s.%s "
                  + "(key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(ttl),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(ttl),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .writeDocument(keyspaceName, tableName, documentId, rows, ttl, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);

      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC INSERT");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(insertCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }
  }

  @Nested
  class UpdateDocument {

    @Test
    public void happyPath() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ?", keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp - 1), Values.of(documentId))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .updateDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC UPDATE");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(100),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(100),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ?", keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp - 1), Values.of(documentId))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .updateDocument(keyspaceName, tableName, documentId, rows, 100, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC UPDATE");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void updateSubPath() {
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp - 1), Values.of(documentId), Values.of("key1"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .updateDocument(keyspaceName, tableName, documentId, subDocumentPath, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC UPDATE");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void updateSubPathRowsNotMatching() {
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      service
          .updateDocument(keyspaceName, tableName, documentId, subDocumentPath, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure(
              throwable ->
                  assertThat(throwable)
                      .isInstanceOf(ErrorCodeRuntimeException.class)
                      .hasFieldOrPropertyWithValue(
                          "errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING));
    }

    @Test
    public void updateSubPathRowsNoPath() {
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .stringValue("value1")
              .build();

      List<JsonShreddedRow> rows = Collections.singletonList(row1);

      service
          .updateDocument(keyspaceName, tableName, documentId, subDocumentPath, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure(
              throwable ->
                  assertThat(throwable)
                      .isInstanceOf(ErrorCodeRuntimeException.class)
                      .hasFieldOrPropertyWithValue(
                          "errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING));
    }
  }

  @Nested
  class PatchDocument {

    @Test
    public void happyPath() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s "
                  + "(key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteExactCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteExactQueryAssert =
          withQuery(
                  deleteExactCql,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deletePatchedKeys =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 IN ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  deletePatchedKeys,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of(Values.of("key1"), Values.of("key2")))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteArray =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  deleteArray,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("[000000]"),
                  Values.of("[999999]"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .patchDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteExactQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteKeysQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteArrayQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC PATCH");
                assertThat(nested.queries())
                    .hasSize(4)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteExactCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deletePatchedKeys, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteArray, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                  + "USING TTL ? AND TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(100),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(100),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteExactCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteExactQueryAssert =
          withQuery(
                  deleteExactCql,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deletePatchedKeys =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 IN ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  deletePatchedKeys,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of(Values.of("key1"), Values.of("key2")))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteArray =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  deleteArray,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("[000000]"),
                  Values.of("[999999]"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .patchDocument(keyspaceName, tableName, documentId, rows, 100, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteExactQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteKeysQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteArrayQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC PATCH");
                assertThat(nested.queries())
                    .hasSize(4)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteExactCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deletePatchedKeys, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteArray, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathSubDocument() {
      List<String> subPath = Collections.singletonList("path");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addAllPath(subPath)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addAllPath(subPath)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("path"),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("path"),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteExactCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteExactQueryAssert =
          withQuery(
                  deleteExactCql,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("path"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deletePatchedKeys =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 IN ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  deletePatchedKeys,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("path"),
                  Values.of(Values.of("key1"), Values.of("key2")))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteArray =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 >= ? AND p1 <= ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  deleteArray,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("path"),
                  Values.of("[000000]"),
                  Values.of("[999999]"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .patchDocument(keyspaceName, tableName, documentId, subPath, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteExactQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteKeysQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteArrayQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC PATCH");
                assertThat(nested.queries())
                    .hasSize(4)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteExactCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deletePatchedKeys, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(
                                  String.format(deleteArray, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void subPathRowsNotMatching() {
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      service
          .patchDocument(keyspaceName, tableName, documentId, subDocumentPath, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure(
              throwable ->
                  assertThat(throwable)
                      .isInstanceOf(ErrorCodeRuntimeException.class)
                      .hasFieldOrPropertyWithValue(
                          "errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING));
    }

    @Test
    public void withArray() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("[000000]")
              .stringValue("value1")
              .build();
      List<JsonShreddedRow> rows = List.of(row1);
      service
          .patchDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure(
              throwable ->
                  assertThat(throwable)
                      .isInstanceOf(ErrorCodeRuntimeException.class)
                      .hasFieldOrPropertyWithValue(
                          "errorCode", ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED));
    }

    @Test
    public void withNoRows() {
      List<JsonShreddedRow> rows = Collections.emptyList();
      service
          .patchDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure(
              throwable ->
                  assertThat(throwable)
                      .isInstanceOf(ErrorCodeRuntimeException.class)
                      .hasFieldOrPropertyWithValue(
                          "errorCode", ErrorCode.DOCS_API_PATCH_EMPTY_NOT_ACCEPTED));
    }
  }

  @Nested
  class SetPathsOnDocument {

    @Test
    public void happyPath() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();

      Set<List<String>> setPaths = new HashSet<>();
      setPaths.add(ImmutableList.of("key1"));
      setPaths.add(ImmutableList.of("key2", "nested"));
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ?",
              keyspaceName, tableName);
      String deleteCql2 =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp - 1), Values.of(documentId), Values.of("key1"))
              .inBatch(expectedBatchType)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert deleteQueryAssert2 =
          withQuery(
                  deleteCql2,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .setPathsOnDocument(keyspaceName, tableName, documentId, setPaths, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert2.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC SET");
                assertThat(nested.queries())
                    .hasSize(3)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql2);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();

      Set<List<String>> setPaths = new HashSet<>();
      setPaths.add(ImmutableList.of("key2", "nested"));
      List<JsonShreddedRow> rows = Arrays.asList(row1);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(100L),
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .returningNothing();

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(
                  deleteCql,
                  Values.of(timestamp - 1),
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"))
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .setPathsOnDocument(keyspaceName, tableName, documentId, setPaths, rows, 100, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC SET");
                assertThat(nested.queries())
                    .hasSize(2)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql())
                              .isEqualTo(String.format(insertCql, keyspaceName + "." + tableName));
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }
  }

  @Nested
  class DeleteDocument {

    @Test
    public void happyPath() {
      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ?", keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp), Values.of(documentId)).returningNothing();

      service
          .deleteDocument(keyspaceName, tableName, documentId, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC DELETE");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }

    @Test
    public void deleteSubPath() {

      List<String> subDocumentPath = Collections.singletonList("key1");

      String deleteCql =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteQueryAssert =
          withQuery(deleteCql, Values.of(timestamp), Values.of(documentId), Values.of("key1"))
              .returningNothing();

      service
          .deleteDocument(keyspaceName, tableName, documentId, subDocumentPath, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      deleteQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC DELETE");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(1);
                        });
              });
    }
  }

  @Nested
  class DeleteDeadLeaves {

    @Test
    public void happyPath() {
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      long microsTimestamp = RandomUtils.nextLong();

      Map<String, Set<DeadLeaf>> deadLeaves = new LinkedHashMap<>();

      LinkedHashSet<DeadLeaf> leavesWithKeys = new LinkedHashSet<>();
      leavesWithKeys.add(ImmutableDeadLeaf.builder().name("key1").build());
      leavesWithKeys.add(ImmutableDeadLeaf.builder().name("key2").build());
      deadLeaves.put("$.a.b", leavesWithKeys);
      deadLeaves.put("$.b", Set.of(DeadLeaf.ARRAY_LEAF));
      deadLeaves.put("$.c.d", Set.of(DeadLeaf.STAR_LEAF));

      String deleteFirst =
          "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 IN ?"
              .formatted(keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteFirstQueryAssert =
          withQuery(
                  deleteFirst,
                  Values.of(microsTimestamp),
                  Values.of(documentId),
                  Values.of("a"),
                  Values.of("b"),
                  Values.of(Values.of("key1"), Values.of("key2")))
              .inBatch(Batch.Type.LOGGED)
              .returningNothing();

      String deleteSecond =
          "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 >= ? AND p1 <= ?"
              .formatted(keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteSecondQueryAssert =
          withQuery(
                  deleteSecond,
                  Values.of(microsTimestamp),
                  Values.of(documentId),
                  Values.of("b"),
                  Values.of("[000000]"),
                  Values.of("[999999]"))
              .inBatch(Batch.Type.LOGGED)
              .returningNothing();

      String deleteThird =
          "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ?"
              .formatted(keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert deleteThirdQueryAssert =
          withQuery(
                  deleteThird,
                  Values.of(microsTimestamp),
                  Values.of(documentId),
                  Values.of("c"),
                  Values.of("d"))
              .inBatch(Batch.Type.LOGGED)
              .returningNothing();

      service
          .deleteDeadLeaves(
              keyspaceName,
              tableName,
              documentId,
              microsTimestamp,
              deadLeaves,
              context,
              new Metadata())
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      deleteFirstQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteSecondQueryAssert.assertExecuteCount().isEqualTo(1);
      deleteThirdQueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC DOCUMENT CORRECTION");
                assertThat(nested.queries())
                    .hasSize(3)
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteFirst);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteSecond);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(deleteThird);
                          assertThat(queryInfo.executionCount()).isEqualTo(1);
                        });
              });
    }
  }

  @Nested
  @TestProfile(WithDifferentConsistency.Profile.class)
  class WithDifferentConsistency {

    public static class Profile extends MaxDepth4TestProfile {

      @Override
      public Map<String, String> getConfigOverrides() {
        return ImmutableMap.<String, String>builder()
            .putAll(super.getConfigOverrides())
            .put("stargate.queries.consistency.writes", "ONE")
            .build();
      }
    }

    @Test
    public void happyPath() {
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      String insertCql =
          String.format(
              "INSERT INTO %s.%s "
                  + "(key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              keyspaceName, tableName);
      ValidatingStargateBridge.QueryAssert row1QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key1"),
                  Values.of(""),
                  Values.of(""),
                  Values.of(""),
                  Values.of("key1"),
                  Values.of("value1"),
                  Values.NULL,
                  Values.NULL,
                  Values.of(timestamp))
              .inBatch(expectedBatchType)
              .withConsistency(QueryOuterClass.Consistency.ONE)
              .returningNothing();
      ValidatingStargateBridge.QueryAssert row2QueryAssert =
          withQuery(
                  insertCql,
                  Values.of(documentId),
                  Values.of("key2"),
                  Values.of("nested"),
                  Values.of(""),
                  Values.of(""),
                  Values.of("nested"),
                  Values.NULL,
                  Values.of(2.2d),
                  Values.NULL,
                  Values.of(timestamp))
              .withConsistency(QueryOuterClass.Consistency.ONE)
              .inBatch(expectedBatchType)
              .returningNothing();

      service
          .writeDocument(keyspaceName, tableName, documentId, rows, null, context)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);

      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC INSERT");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.cql()).isEqualTo(insertCql);
                          assertThat(queryInfo.executionCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }
  }
}
