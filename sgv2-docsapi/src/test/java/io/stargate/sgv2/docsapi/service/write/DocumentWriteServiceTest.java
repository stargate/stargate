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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.bridge.AbstractBridgeTest;
import io.stargate.sgv2.docsapi.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class DocumentWriteServiceTest extends AbstractBridgeTest {

  @Inject DocumentWriteService service;
  @Inject DocsApiTestSchemaProvider schemaProvider;
  @Inject DataStoreProperties dataStoreProperties;
  @Inject DocumentProperties documentProperties;
  @InjectMock TimeSource timeSource;

  ExecutionContext context;

  @BeforeEach
  public void init() {
    context = ExecutionContext.create(true);
  }

  @Nested
  class WriteDocument {

    @Test
    public void happyPath() {
      String keyspaceName = schemaProvider.getKeyspace().getName();
      String tableName = schemaProvider.getTable().getName();
      Batch.Type expectedBatchType =
          dataStoreProperties.loggedBatchesEnabled() ? Batch.Type.LOGGED : Batch.Type.UNLOGGED;
      String documentId = RandomStringUtils.randomAlphanumeric(16);
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

      long timestamp = RandomUtils.nextLong();
      when(timeSource.currentTimeMicros()).thenReturn(timestamp);

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
          .writeDocument(keyspaceName, tableName, documentId, rows, null, false, context)
          .await()
          .atMost(Duration.ofSeconds(1));

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
                          assertThat(queryInfo.preparedCQL()).isEqualTo(insertCql);
                          assertThat(queryInfo.execCount()).isEqualTo(2);
                          // TODO remove if we can't implement rowCount (see ProfilingContext)
                          // assertThat(queryInfo.rowCount()).isEqualTo(2);
                        });
              });
    }
  }
}
