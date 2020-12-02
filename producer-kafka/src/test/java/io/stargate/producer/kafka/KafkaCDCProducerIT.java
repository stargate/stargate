/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import static io.stargate.producer.kafka.helpers.MutationEventHelper.cell;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.cellValue;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemaConstants.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME_2;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.PARTITION_KEY_NAME;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableListType;
import io.stargate.db.schema.ImmutableMapType;
import io.stargate.db.schema.ImmutableSetType;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.producer.kafka.health.KafkaHealthCheck;
import io.stargate.producer.kafka.health.SchemaRegistryHealthCheck;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KafkaCDCProducerIT extends IntegrationTestBase {

  @Test
  public void shouldSendUpdateEventWithOnePartitionKeyAndOneValue() throws Exception {
    // given
    String columnValue = "col_value";
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event
    mockOnePKOneCKOneColumn(tableMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();

    // send actual event
    RowUpdateEvent rowMutationEvent =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Column.Type.Text),
            columnValue,
            column(COLUMN_NAME, Column.Type.Text),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int),
            tableMetadata,
            1000);
    kafkaCDCProducer.send(rowMutationEvent).get();

    // then
    GenericRecord expectedKey =
        kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
    GenericRecord expectedValue =
        kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

    try {
      verifyReceivedByKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldSendDeleteEventForAllPKsAndCK() throws Exception {
    // given
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event
    mockOnePKOneCKOneColumn(tableMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata);

    // end delete event
    DeleteEvent event =
        createDeleteEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME),
            tableMetadata);
    kafkaCDCProducer.send(event).get();

    // then
    GenericRecord expectedKey = kafkaCDCProducer.keyValueConstructor.constructKey(event, topicName);
    GenericRecord expectedValue =
        kafkaCDCProducer.keyValueConstructor.constructValue(event, topicName);

    try {
      verifyReceivedByKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @ParameterizedTest
  @MethodSource("columnsAfterChange")
  public void shouldSendUpdateAndSendSecondEventWhenSchemaChanged(
      List<Column> metadataAfterChange, List<Cell> columnsAfterChange) throws Exception {
    // given
    String columnValue = "col_value";
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event
    mockOnePKOneCKOneColumn(tableMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              PARTITION_KEY_VALUE,
              partitionKey(PARTITION_KEY_NAME, Column.Type.Text),
              columnValue,
              column(COLUMN_NAME, Column.Type.Text),
              CLUSTERING_KEY_VALUE,
              clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int),
              tableMetadata);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      verifyReceivedByKafka(expectedKey, expectedValue, topicName);

      // when change schema
      mockOnePKOneCKAndColumns(tableMetadata, metadataAfterChange);
      kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();

      // and send event with a new column
      rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(
                      PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Column.Type.Text))),
              columnsAfterChange,
              Collections.singletonList(
                  cellValue(
                      CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int))),
              tableMetadata);

      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      expectedKey = kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      verifyReceivedByKafka(expectedKey, expectedValue, topicName);

    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @ParameterizedTest
  @MethodSource("nativeTypesProvider")
  public void shouldSendEventsWithAllNativeTypes(
      List<Column> columnMetadata, List<Cell> columnValues) throws Exception {
    // given
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event
    mockOnePKOneCKAndColumns(tableMetadata, columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(
                      PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Column.Type.Text))),
              columnValues,
              Collections.singletonList(
                  cellValue(
                      CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int))),
              tableMetadata);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      verifyReceivedByKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  public static Stream<Arguments> columnsAfterChange() {
    String columnValue = "value";
    return Stream.of(
        Arguments.of(
            Arrays.asList(
                column(COLUMN_NAME, Column.Type.Text), column(COLUMN_NAME_2, Column.Type.Text)),
            Arrays.asList(
                cell(column(COLUMN_NAME, Column.Type.Text), columnValue),
                cell(column(COLUMN_NAME_2, Column.Type.Text), columnValue)) // add new column
            ),
        Arguments.of(
            Collections.emptyList(), Collections.emptyList() // remove columns
            ),
        Arguments.of(
            Collections.singletonList(column(COLUMN_NAME + "_renamed", Column.Type.Text)),
            Collections.singletonList(
                cell(
                    column(COLUMN_NAME + "_renamed", Column.Type.Text),
                    columnValue))) // rename column
        );
  }

  public static Stream<Arguments> nativeTypesProvider() {
    InetAddress address;
    try {
      address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    } catch (UnknownHostException ex) {
      throw new AssertionError("Could not get address from 127.0.0.1", ex);
    }

    return Stream.of(
        Arguments.of(
            Collections.singletonList(column(Column.Type.Ascii)),
            Collections.singletonList(cell(column(Column.Type.Ascii), "ascii"))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Bigint)),
            Collections.singletonList(cell(column(Column.Type.Ascii), Long.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Blob)),
            Collections.singletonList(
                cell(column(Column.Type.Blob), Bytes.fromHexString("0xCAFE")))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Boolean)),
            Collections.singletonList(cell(column(Column.Type.Boolean), true))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Counter)),
            Collections.singletonList(cell(column(Column.Type.Counter), 1L))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Date)),
            Collections.singletonList(cell(column(Column.Type.Date), LocalDate.ofEpochDay(16071)))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Decimal)),
            Collections.singletonList(
                cell(column(Column.Type.Decimal), new BigDecimal("12.3E+7")))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Double)),
            Collections.singletonList(cell(column(Column.Type.Double), Double.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Duration)),
            Collections.singletonList(
                cell(column(Column.Type.Duration), CqlDuration.from("PT30H20M")))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Float)),
            Collections.singletonList(cell(column(Column.Type.Float), Float.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Inet)),
            Collections.singletonList(cell(column(Column.Type.Inet), address))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Int)),
            Collections.singletonList(cell(column(Column.Type.Int), Integer.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Smallint)),
            Collections.singletonList(cell(column(Column.Type.Smallint), Short.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Text)),
            Collections.singletonList(cell(column(Column.Type.Text), "text"))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Time)),
            Collections.singletonList(
                cell(column(Column.Type.Time), LocalTime.ofNanoOfDay(54012123450000L)))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Timestamp)),
            Collections.singletonList(
                cell(column(Column.Type.Timestamp), Instant.ofEpochMilli(872835240000L)))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Timeuuid)),
            Collections.singletonList(
                cell(
                    column(Column.Type.Timeuuid),
                    UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66")))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Tinyint)),
            Collections.singletonList(cell(column(Column.Type.Tinyint), Byte.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Uuid)),
            Collections.singletonList(
                cell(
                    column(Column.Type.Uuid),
                    UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Varchar)),
            Collections.singletonList(cell(column(Column.Type.Varchar), "varchar"))),
        Arguments.of(
            Collections.singletonList(column(Column.Type.Varint)),
            Collections.singletonList(
                cell(column(Column.Type.Varint), new BigInteger(Integer.MAX_VALUE + "000")))));
  }

  @ParameterizedTest
  @MethodSource("complexTypesProvider")
  public void shouldSendEventsWithComplexTypes(List<Column> columnMetadata, List<Cell> columnValues)
      throws Exception {
    // given
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event
    mockOnePKOneCKAndColumns(tableMetadata, columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(
                      PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Column.Type.Text))),
              columnValues,
              Collections.singletonList(
                  cellValue(
                      CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int))),
              tableMetadata);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      verifyReceivedByKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldSendEventsWithMapAndNestedMap() throws Exception {
    // given
    Table tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // normal map
    ParameterizedType.MapType mapType =
        ImmutableMapType.builder().addParameters(Column.Type.Int, Column.Type.Decimal).build();
    Map<Integer, BigDecimal> mapValues = ImmutableMap.of(123, new BigDecimal(123));
    // Utf8 as key because avro converts all keys to this type
    Map<Utf8, BigDecimal> expectedMapValues = ImmutableMap.of(new Utf8("123"), new BigDecimal(123));

    // nested map
    ParameterizedType.MapType mapOfMapType =
        ImmutableMapType.builder()
            .addParameters(
                Column.Type.Int,
                ImmutableMapType.builder().addParameters(Column.Type.Text, Column.Type.Int).build())
            .build();
    Map<Integer, Map<String, Integer>> mapOfMapValues =
        ImmutableMap.of(4, ImmutableMap.of("v", 100));
    Map<Utf8, Map<Utf8, Integer>> expectedMapOfMapValues =
        ImmutableMap.of(new Utf8("4"), ImmutableMap.of(new Utf8("v"), 100));

    // when
    // schema change event
    when(tableMetadata.partitionKeyColumns())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Column.Type.Text)));
    when(tableMetadata.clusteringKeyColumns())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int)));
    when(tableMetadata.columns())
        .thenReturn(
            Arrays.asList(column(COLUMN_NAME, mapType), column(COLUMN_NAME_2, mapOfMapType)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(
                      PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Column.Type.Text))),
              Arrays.asList(
                  cell(column(COLUMN_NAME, mapType), mapValues),
                  cell(column(COLUMN_NAME_2, mapOfMapType), mapOfMapValues)),
              Collections.singletonList(
                  cellValue(
                      CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int))),
              tableMetadata);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);
      // override map with expected type
      ((GenericRecord) ((GenericRecord) expectedValue.get(DATA_FIELD_NAME)).get(COLUMN_NAME))
          .put(0, expectedMapValues);
      ((GenericRecord) ((GenericRecord) expectedValue.get(DATA_FIELD_NAME)).get(COLUMN_NAME_2))
          .put(0, expectedMapOfMapValues);

      verifyReceivedByKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldSendUpdateEventToNTopicsBasedOnTheTableMetadata() throws Exception {
    // given
    Table tableMetadataFirst = mockTableMetadata();
    Table tableMetadataSecond = mockTableMetadata();
    String topicNameFirst = createTopicName(tableMetadataFirst);
    String topicNameSecond = createTopicName(tableMetadataFirst);

    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, new HealthCheckRegistry());
    kafkaCDCProducer.init().get();

    // when
    // schema change event for two independent topics - both have different schema
    mockOnePKOneCKAndColumns(
        tableMetadataFirst, Collections.singletonList(column(COLUMN_NAME, Column.Type.Text)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadataFirst).get();

    mockOnePKOneCKAndColumns(
        tableMetadataSecond, Collections.singletonList(column(COLUMN_NAME_2, Column.Type.Int)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadataSecond).get();

    // send events to two independent topics
    RowUpdateEvent rowMutationEventFirstTopic =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Column.Type.Text),
            "col_value",
            column(COLUMN_NAME, Column.Type.Text),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int),
            tableMetadataFirst,
            1000);
    kafkaCDCProducer.send(rowMutationEventFirstTopic).get();

    RowUpdateEvent rowMutationEventSecondTopic =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Column.Type.Text),
            123456,
            column(COLUMN_NAME_2, Column.Type.Int),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int),
            tableMetadataSecond,
            1000);
    kafkaCDCProducer.send(rowMutationEventSecondTopic).get();

    // then
    GenericRecord expectedKeyFirstTopic =
        kafkaCDCProducer.keyValueConstructor.constructKey(
            rowMutationEventFirstTopic, topicNameFirst);
    GenericRecord expectedValueFirstTopic =
        kafkaCDCProducer.keyValueConstructor.constructValue(
            rowMutationEventFirstTopic, topicNameFirst);

    GenericRecord expectedKeySecondTopic =
        kafkaCDCProducer.keyValueConstructor.constructKey(
            rowMutationEventFirstTopic, topicNameSecond);
    GenericRecord expectedValueSecondTopic =
        kafkaCDCProducer.keyValueConstructor.constructValue(
            rowMutationEventFirstTopic, topicNameSecond);

    try {
      verifyReceivedByKafka(expectedKeyFirstTopic, expectedValueFirstTopic, topicNameFirst);
      verifyReceivedByKafka(expectedKeySecondTopic, expectedValueSecondTopic, topicNameSecond);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldRegisterHealthChecks() throws Exception {
    // given
    HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
    ConfigStore configStore = mockConfigStoreWithProducerSettings();
    KafkaCDCProducer kafkaCDCProducer =
        new KafkaCDCProducer(new MetricRegistry(), configStore, healthCheckRegistry);
    kafkaCDCProducer.init().get();

    // when
    HealthCheck kafkaHealthCheck =
        healthCheckRegistry.getHealthCheck(KafkaHealthCheck.KAFKA_HEALTH_CHECK_PREFIX);
    HealthCheck schemaRegistryHealthCheck =
        healthCheckRegistry.getHealthCheck(
            SchemaRegistryHealthCheck.SCHEMA_REGISTRY_HEALTH_CHECK_PREFIX);

    // then
    try {
      assertThat(kafkaHealthCheck.execute().isHealthy()).isTrue();
      assertThat(schemaRegistryHealthCheck.execute().isHealthy()).isTrue();
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  private void mockOnePKOneCKOneColumn(Table tableMetadata) {
    mockOnePKOneCKAndColumns(
        tableMetadata, Collections.singletonList(column(COLUMN_NAME, Column.Type.Text)));
  }

  private void mockOnePKOneCKAndColumns(Table tableMetadata, List<Column> columnMetadata) {
    when(tableMetadata.partitionKeyColumns())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Column.Type.Text)));
    when(tableMetadata.clusteringKeyColumns())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Column.Type.Int)));
    when(tableMetadata.columns()).thenReturn(columnMetadata);
  }

  @SuppressWarnings("unchecked")
  public static Stream<Arguments> complexTypesProvider() {
    ParameterizedType.ListType listType =
        ImmutableListType.builder().addParameters(Column.Type.Int).build();
    ParameterizedType.ListType listOfSet =
        ImmutableListType.builder()
            .addParameters(ImmutableSetType.builder().addParameters(Column.Type.Int).build())
            .build();
    ParameterizedType.SetType setType =
        ImmutableSetType.builder().addParameters(Column.Type.Int).build();
    ParameterizedType.SetType setOfList =
        ImmutableSetType.builder()
            .addParameters(ImmutableListType.builder().addParameters(Column.Type.Int).build())
            .build();
    // udt
    Column[] udtColumns = {
      ImmutableColumn.builder().name("udtcol_1").type(Column.Type.Int).build(),
      ImmutableColumn.builder().name("udtcol_2").type(Column.Type.Text).build(),
    };
    UserDefinedType userDefinedType =
        ImmutableUserDefinedType.builder()
            .keyspace("ks")
            .name("typeName")
            .addColumns(udtColumns)
            .build();
    Map<String, Object> udtValue =
        ImmutableMap.<String, Object>builder().put("udtcol_1", 47).put("udtcol_2", "value").build();
    // nested udt

    Column[] nestedUdtColumns = {
      ImmutableColumn.builder().name("nested").type(userDefinedType).build(),
      ImmutableColumn.builder()
          .name("list")
          .type(ImmutableListType.builder().addParameters(Column.Type.Int).build())
          .build()
    };

    UserDefinedType userDefinedTypeNested =
        ImmutableUserDefinedType.builder()
            .keyspace("ks")
            .name("nested")
            .addColumns(nestedUdtColumns)
            .build();

    Map<String, Object> nestedUdtValue =
        ImmutableMap.<String, Object>builder()
            .put("nested", udtValue)
            .put("list", Arrays.asList(1, 2))
            .build();

    ColumnType customType = customType();

    // tuple
    ParameterizedType.TupleType tupleType =
        ImmutableTupleType.builder()
            .addParameters(
                Column.Type.Int,
                ImmutableListType.builder().addParameters(Column.Type.Text).build(),
                Column.Type.Decimal)
            .build();
    List<Object> tupleValues =
        Arrays.asList(1, Collections.singletonList("v_1"), new BigDecimal(100));

    // nested tuple
    ParameterizedType.TupleType nestedTupleType =
        ImmutableTupleType.builder().addParameters(Column.Type.Int, tupleType).build();
    List<Object> nestedTupleValues = Arrays.asList(1, tupleValues);

    return Stream.of(
        Arguments.of(
            Collections.singletonList(column(listType)),
            Collections.singletonList(cell(column(listType), Arrays.asList(1, 2)))),
        Arguments.of(
            Collections.singletonList(column(listOfSet)),
            Collections.singletonList(
                cell(column(listOfSet), Arrays.asList(Sets.newHashSet(1), Sets.newHashSet(2))))),
        Arguments.of(
            Collections.singletonList(column(setType)),
            Collections.singletonList(cell(column(setType), Sets.newHashSet(1)))),
        Arguments.of(
            Collections.singletonList(column(setOfList)),
            Collections.singletonList(
                cell(
                    column(setOfList),
                    Sets.newHashSet(Collections.singletonList(1), Collections.singletonList(2))))),
        Arguments.of(
            Collections.singletonList(column(userDefinedType)),
            Collections.singletonList(cell(column(userDefinedType), udtValue))),
        Arguments.of(
            Collections.singletonList(column(userDefinedTypeNested)),
            Collections.singletonList(cell(column(userDefinedTypeNested), nestedUdtValue))),
        Arguments.of(
            Collections.singletonList(column(customType)),
            Collections.singletonList(cell(column(customType), Bytes.fromHexString("0xCAFE")))),
        Arguments.of(
            Collections.singletonList(column(tupleType)),
            Collections.singletonList(cell(column(tupleType), tupleValues))),
        Arguments.of(
            Collections.singletonList(column(nestedTupleType)),
            Collections.singletonList(cell(column(nestedTupleType), nestedTupleValues))));
  }

  @NotNull
  private static ColumnType customType() {
    return new ColumnType() {

      @Override
      public int id() {
        // 0 is the code for custom types
        return 0;
      }

      @Override
      public Column.Type rawType() {
        return null;
      }

      @Override
      public Class<?> javaType() {
        return null;
      }

      @Override
      public String cqlDefinition() {
        return null;
      }

      @Override
      public String name() {
        return null;
      }

      @Override
      public TypeCodec codec() {
        return null;
      }

      @Override
      public ColumnType fieldType(String name) {
        return null;
      }
    };
  }
}
