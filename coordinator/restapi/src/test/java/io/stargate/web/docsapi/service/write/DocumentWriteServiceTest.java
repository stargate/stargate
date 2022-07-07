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

package io.stargate.web.docsapi.service.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import io.stargate.core.util.TimeSource;
import io.stargate.db.BatchType;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.ValidatingDataStore;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentWriteServiceTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 4;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  DocumentWriteService service;

  ExecutionContext context;

  @Mock DocsApiConfiguration configuration;

  @Mock TimeSource timeSource;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @BeforeEach
  public void init() {
    when(configuration.getMaxDepth()).thenReturn(MAX_DEPTH);
    context = ExecutionContext.create(true);
    service = new DocumentWriteService(timeSource, configuration);
  }

  @Nested
  class WriteDocument {

    @Test
    public void happyPath() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.writeDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, null, false, context);

      result.test().await().assertValueCount(1).assertComplete();
      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC INSERT");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.writeDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, 100, false, context);

      result.test().await().assertValueCount(1).assertComplete();
      row1QueryAssert.assertExecuteCount().isEqualTo(1);
      row2QueryAssert.assertExecuteCount().isEqualTo(1);

      // execution context
      assertThat(context.toProfile().nested())
          .singleElement()
          .satisfies(
              nested -> {
                assertThat(nested.description()).isEqualTo("ASYNC INSERT");
                assertThat(nested.queries())
                    .singleElement()
                    .satisfies(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }
  }

  @Nested
  class UpdateDocument {

    @Test
    public void happyPath() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteCql = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?";
      ValidatingDataStore.QueryAssert deleteQueryAssert =
          withQuery(SCHEMA_PROVIDER.getTable(), deleteCql, timestamp - 1, documentId)
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.updateDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, null, false, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(deleteCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteCql = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?";
      ValidatingDataStore.QueryAssert deleteQueryAssert =
          withQuery(SCHEMA_PROVIDER.getTable(), deleteCql, timestamp - 1, documentId)
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.updateDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, 100, false, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(deleteCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void updateSubPath() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteCql = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ?";
      ValidatingDataStore.QueryAssert deleteQueryAssert =
          withQuery(SCHEMA_PROVIDER.getTable(), deleteCql, timestamp - 1, documentId, "key1")
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.updateDocument(
              datastore,
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              subDocumentPath,
              rows,
              null,
              false,
              context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(deleteCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void updateSubPathRowsNotMatching() {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      Throwable throwable =
          catchThrowable(
              () ->
                  service.updateDocument(
                      datastore,
                      KEYSPACE_NAME,
                      COLLECTION_NAME,
                      documentId,
                      subDocumentPath,
                      rows,
                      null,
                      false,
                      context));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING);
    }

    @Test
    public void updateSubPathRowsNoPath() {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder().maxDepth(MAX_DEPTH).stringValue("value1").build();

      List<JsonShreddedRow> rows = Collections.singletonList(row1);

      Throwable throwable =
          catchThrowable(
              () ->
                  service.updateDocument(
                      datastore,
                      KEYSPACE_NAME,
                      COLLECTION_NAME,
                      documentId,
                      subDocumentPath,
                      rows,
                      null,
                      false,
                      context));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING);
    }
  }

  @Nested
  class PatchDocument {

    @Test
    public void happyPath() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteExactCql =
          "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?";
      ValidatingDataStore.QueryAssert deleteExactQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteExactCql,
                  timestamp - 1,
                  documentId,
                  "",
                  "",
                  "",
                  "")
              .inBatch(batchType)
              .returningNothing();

      String deletePatchedKeys = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 IN ?";
      ValidatingDataStore.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deletePatchedKeys,
                  timestamp - 1,
                  documentId,
                  Arrays.asList("key1", "key2"))
              .inBatch(batchType)
              .returningNothing();

      String deleteArray = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?";
      ValidatingDataStore.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteArray,
                  timestamp - 1,
                  documentId,
                  "[000000]",
                  "[999999]")
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.patchDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, null, false, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteExactCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deletePatchedKeys, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteArray, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathWithTtl() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key1",
                  "",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "key2",
                  "nested",
                  "",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  100,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteExactCql =
          "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?";
      ValidatingDataStore.QueryAssert deleteExactQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteExactCql,
                  timestamp - 1,
                  documentId,
                  "",
                  "",
                  "",
                  "")
              .inBatch(batchType)
              .returningNothing();

      String deletePatchedKeys = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 IN ?";
      ValidatingDataStore.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deletePatchedKeys,
                  timestamp - 1,
                  documentId,
                  Arrays.asList("key1", "key2"))
              .inBatch(batchType)
              .returningNothing();

      String deleteArray = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?";
      ValidatingDataStore.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteArray,
                  timestamp - 1,
                  documentId,
                  "[000000]",
                  "[999999]")
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.patchDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, rows, 100, false, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteExactCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deletePatchedKeys, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteArray, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void happyPathSubDocument() throws Exception {
      DataStore datastore = datastore();
      BatchType batchType =
          datastore.supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
      List<String> subPath = Collections.singletonList("path");
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addAllPath(subPath)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addAllPath(subPath)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String insertCql =
          "INSERT INTO %s (key, p0, p1, p2, p3, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
      ValidatingDataStore.QueryAssert row1QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "path",
                  "key1",
                  "",
                  "",
                  "key1",
                  "value1",
                  null,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();
      ValidatingDataStore.QueryAssert row2QueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  insertCql,
                  documentId,
                  "path",
                  "key2",
                  "nested",
                  "",
                  "nested",
                  null,
                  2.2d,
                  null,
                  timestamp)
              .inBatch(batchType)
              .returningNothing();

      String deleteExactCql =
          "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?";
      ValidatingDataStore.QueryAssert deleteExactQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteExactCql,
                  timestamp - 1,
                  documentId,
                  "path",
                  "",
                  "",
                  "")
              .inBatch(batchType)
              .returningNothing();

      String deletePatchedKeys =
          "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 IN ?";
      ValidatingDataStore.QueryAssert deleteKeysQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deletePatchedKeys,
                  timestamp - 1,
                  documentId,
                  "path",
                  Arrays.asList("key1", "key2"))
              .inBatch(batchType)
              .returningNothing();

      String deleteArray =
          "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 >= ? AND p1 <= ?";
      ValidatingDataStore.QueryAssert deleteArrayQueryAssert =
          withQuery(
                  SCHEMA_PROVIDER.getTable(),
                  deleteArray,
                  timestamp - 1,
                  documentId,
                  "path",
                  "[000000]",
                  "[999999]")
              .inBatch(batchType)
              .returningNothing();

      Single<ResultSet> result =
          service.patchDocument(
              datastore,
              KEYSPACE_NAME,
              COLLECTION_NAME,
              documentId,
              subPath,
              rows,
              null,
              false,
              context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteExactCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deletePatchedKeys, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(
                                      deleteArray, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                        })
                    .anySatisfy(
                        queryInfo -> {
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(insertCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }

    @Test
    public void subPathRowsNotMatching() {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("key1");
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key1")
              .stringValue("value1")
              .build();
      JsonShreddedRow row2 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("key2")
              .addPath("nested")
              .doubleValue(2.2d)
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1, row2);

      Throwable throwable =
          catchThrowable(
              () ->
                  service.patchDocument(
                      datastore,
                      KEYSPACE_NAME,
                      COLLECTION_NAME,
                      documentId,
                      subDocumentPath,
                      rows,
                      null,
                      false,
                      context));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING);
    }

    @Test
    public void withArray() {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row1 =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("[000000]")
              .stringValue("value1")
              .build();
      List<JsonShreddedRow> rows = Arrays.asList(row1);

      Throwable throwable =
          catchThrowable(
              () ->
                  service.patchDocument(
                      datastore,
                      KEYSPACE_NAME,
                      COLLECTION_NAME,
                      documentId,
                      rows,
                      null,
                      false,
                      context));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
    }

    @Test
    public void withNoRows() {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<JsonShreddedRow> rows = Collections.emptyList();

      Throwable throwable =
          catchThrowable(
              () ->
                  service.patchDocument(
                      datastore,
                      KEYSPACE_NAME,
                      COLLECTION_NAME,
                      documentId,
                      rows,
                      null,
                      false,
                      context));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_PATCH_EMPTY_NOT_ACCEPTED);
    }
  }

  @Nested
  class DeleteDocument {

    @Test
    public void happyPath() throws Exception {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String deleteCql = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?";
      ValidatingDataStore.QueryAssert deleteQueryAssert =
          withQuery(SCHEMA_PROVIDER.getTable(), deleteCql, timestamp - 1, documentId)
              .returningNothing();

      Single<ResultSet> result =
          service.deleteDocument(datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(deleteCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }

    @Test
    public void deleteSubPath() throws Exception {
      DataStore datastore = datastore();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      List<String> subDocumentPath = Collections.singletonList("key1");

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

      String deleteCql = "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ?";
      ValidatingDataStore.QueryAssert deleteQueryAssert =
          withQuery(SCHEMA_PROVIDER.getTable(), deleteCql, timestamp - 1, documentId, "key1")
              .returningNothing();

      Single<ResultSet> result =
          service.deleteDocument(
              datastore, KEYSPACE_NAME, COLLECTION_NAME, documentId, subDocumentPath, context);

      result.test().await().assertValueCount(1).assertComplete();
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
                          assertThat(queryInfo.preparedCQL())
                              .isEqualTo(
                                  String.format(deleteCql, KEYSPACE_NAME + "." + COLLECTION_NAME));
                          assertThat(queryInfo.execCount()).isEqualTo(1);
                          assertThat(queryInfo.rowCount()).isEqualTo(0);
                        });
              });
    }
  }
}
