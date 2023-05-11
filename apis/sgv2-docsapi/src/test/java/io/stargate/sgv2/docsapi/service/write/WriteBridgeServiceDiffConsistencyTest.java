package io.stargate.sgv2.docsapi.service.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(WriteBridgeServiceDiffConsistencyTest.Profile.class)
public class WriteBridgeServiceDiffConsistencyTest extends AbstractValidatingStargateBridgeTest {

  public static class Profile extends MaxDepth4TestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("stargate.queries.consistency.writes", "ONE")
          .build();
    }
  }

  @Inject WriteBridgeService service;
  @Inject DocsApiTestSchemaProvider schemaProvider;
  @Inject DataStoreProperties dataStoreProperties;
  @Inject DocumentProperties documentProperties;
  @InjectMock TimeSource timeSource;

  @Test
  public void happyPath() {
    String keyspaceName = schemaProvider.getKeyspace().getName();
    String tableName = schemaProvider.getTable().getName();
    QueryOuterClass.Batch.Type expectedBatchType =
        dataStoreProperties.loggedBatchesEnabled()
            ? QueryOuterClass.Batch.Type.LOGGED
            : QueryOuterClass.Batch.Type.UNLOGGED;
    String documentId = RandomStringUtils.randomAlphanumeric(16);
    long timestamp = RandomUtils.nextLong();
    ExecutionContext context = ExecutionContext.create(true);

    when(timeSource.currentTimeMicros()).thenReturn(timestamp);

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

    Assertions.assertThat(context.toProfile().nested())
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
