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
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Collection.Kind;
import org.apache.cassandra.stargate.schema.CQLType.Custom;
import org.apache.cassandra.stargate.schema.CQLType.MapDataType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.CQLType.Tuple;
import org.apache.cassandra.stargate.schema.CQLType.UserDefined;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KafkaCDCProducerIntegrationTest extends IntegrationTestBase {

  @Test
  public void shouldSendUpdateEventWithOnePartitionKeyAndOneValue() throws Exception {
    // given
    String columnValue = "col_value";
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    mockOnePKOneCKOneColumn(tableMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();

    // send actual event
    RowUpdateEvent rowMutationEvent =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            columnValue,
            column(COLUMN_NAME, Native.TEXT),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
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
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

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
      List<ColumnMetadata> metadataAfterChange, List<Cell> columnsAfterChange) throws Exception {
    // given
    String columnValue = "col_value";
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    mockOnePKOneCKOneColumn(tableMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              PARTITION_KEY_VALUE,
              partitionKey(PARTITION_KEY_NAME, Native.TEXT),
              columnValue,
              column(COLUMN_NAME, Native.TEXT),
              CLUSTERING_KEY_VALUE,
              clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
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
                  cellValue(PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnsAfterChange,
              Collections.singletonList(
                  cellValue(CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
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
      List<ColumnMetadata> columnMetadata, List<Cell> columnValues) throws Exception {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    mockOnePKOneCKAndColumns(tableMetadata, columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnValues,
              Collections.singletonList(
                  cellValue(CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
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
            Arrays.asList(column(COLUMN_NAME, Native.TEXT), column(COLUMN_NAME_2, Native.TEXT)),
            Arrays.asList(
                cell(column(COLUMN_NAME, Native.TEXT), columnValue),
                cell(column(COLUMN_NAME_2, Native.TEXT), columnValue)) // add new column
            ),
        Arguments.of(
            Collections.emptyList(), Collections.emptyList() // remove columns
            ),
        Arguments.of(
            Collections.singletonList(column(COLUMN_NAME + "_renamed", Native.TEXT)),
            Collections.singletonList(
                cell(column(COLUMN_NAME + "_renamed", Native.TEXT), columnValue))) // rename column
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
            Collections.singletonList(column(Native.ASCII)),
            Collections.singletonList(cell(column(Native.ASCII), "ascii"))),
        Arguments.of(
            Collections.singletonList(column(Native.BIGINT)),
            Collections.singletonList(cell(column(Native.ASCII), Long.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.BLOB)),
            Collections.singletonList(cell(column(Native.BLOB), Bytes.fromHexString("0xCAFE")))),
        Arguments.of(
            Collections.singletonList(column(Native.BOOLEAN)),
            Collections.singletonList(cell(column(Native.BOOLEAN), true))),
        Arguments.of(
            Collections.singletonList(column(Native.COUNTER)),
            Collections.singletonList(cell(column(Native.COUNTER), 1L))),
        Arguments.of(
            Collections.singletonList(column(Native.DATE)),
            Collections.singletonList(cell(column(Native.DATE), LocalDate.ofEpochDay(16071)))),
        Arguments.of(
            Collections.singletonList(column(Native.DECIMAL)),
            Collections.singletonList(cell(column(Native.DECIMAL), new BigDecimal("12.3E+7")))),
        Arguments.of(
            Collections.singletonList(column(Native.DOUBLE)),
            Collections.singletonList(cell(column(Native.DOUBLE), Double.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.DURATION)),
            Collections.singletonList(cell(column(Native.DURATION), CqlDuration.from("PT30H20M")))),
        Arguments.of(
            Collections.singletonList(column(Native.FLOAT)),
            Collections.singletonList(cell(column(Native.FLOAT), Float.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.INET)),
            Collections.singletonList(cell(column(Native.INET), address))),
        Arguments.of(
            Collections.singletonList(column(Native.INT)),
            Collections.singletonList(cell(column(Native.INT), Integer.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.SMALLINT)),
            Collections.singletonList(cell(column(Native.SMALLINT), Short.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.TEXT)),
            Collections.singletonList(cell(column(Native.TEXT), "text"))),
        Arguments.of(
            Collections.singletonList(column(Native.TIME)),
            Collections.singletonList(
                cell(column(Native.TIME), LocalTime.ofNanoOfDay(54012123450000L)))),
        Arguments.of(
            Collections.singletonList(column(Native.TIMESTAMP)),
            Collections.singletonList(
                cell(column(Native.TIMESTAMP), Instant.ofEpochMilli(872835240000L)))),
        Arguments.of(
            Collections.singletonList(column(Native.TIMEUUID)),
            Collections.singletonList(
                cell(
                    column(Native.TIMEUUID),
                    UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66")))),
        Arguments.of(
            Collections.singletonList(column(Native.TINYINT)),
            Collections.singletonList(cell(column(Native.TINYINT), Byte.MAX_VALUE))),
        Arguments.of(
            Collections.singletonList(column(Native.UUID)),
            Collections.singletonList(
                cell(
                    column(Native.UUID), UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")))),
        Arguments.of(
            Collections.singletonList(column(Native.VARCHAR)),
            Collections.singletonList(cell(column(Native.VARCHAR), "varchar"))),
        Arguments.of(
            Collections.singletonList(column(Native.VARINT)),
            Collections.singletonList(
                cell(column(Native.VARINT), new BigInteger(Integer.MAX_VALUE + "000")))));
  }

  @ParameterizedTest
  @MethodSource("complexTypesProvider")
  public void shouldSendEventsWithComplexTypes(
      List<ColumnMetadata> columnMetadata, List<Cell> columnValues) throws Exception {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    mockOnePKOneCKAndColumns(tableMetadata, columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnValues,
              Collections.singletonList(
                  cellValue(CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
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
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // normal map
    MapDataType mapType = new MapDataType(Native.INT, Native.DECIMAL);
    Map<Integer, BigDecimal> mapValues = ImmutableMap.of(123, new BigDecimal(123));
    // Utf8 as key because avro converts all keys to this type
    Map<Utf8, BigDecimal> expectedMapValues = ImmutableMap.of(new Utf8("123"), new BigDecimal(123));

    // nested map
    MapDataType mapOfMapType =
        new MapDataType(Native.INT, new MapDataType(Native.TEXT, Native.INT));
    Map<Integer, Map<String, Integer>> mapOfMapValues =
        ImmutableMap.of(4, ImmutableMap.of("v", 100));
    Map<Utf8, Map<Utf8, Integer>> expectedMapOfMapValues =
        ImmutableMap.of(new Utf8("4"), ImmutableMap.of(new Utf8("v"), 100));

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns())
        .thenReturn(
            Arrays.asList(column(COLUMN_NAME, mapType), column(COLUMN_NAME_2, mapOfMapType)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(PARTITION_KEY_VALUE, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              Arrays.asList(
                  cell(column(COLUMN_NAME, mapType), mapValues),
                  cell(column(COLUMN_NAME_2, mapOfMapType), mapOfMapValues)),
              Collections.singletonList(
                  cellValue(CLUSTERING_KEY_VALUE, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
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
    TableMetadata tableMetadataFirst = mockTableMetadata();
    TableMetadata tableMetadataSecond = mockTableMetadata();
    String topicNameFirst = createTopicName(tableMetadataFirst);
    String topicNameSecond = createTopicName(tableMetadataFirst);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(new MetricRegistry());
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event for two independent topics - both have different schema
    mockOnePKOneCKAndColumns(
        tableMetadataFirst, Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadataFirst).get();

    mockOnePKOneCKAndColumns(
        tableMetadataSecond, Collections.singletonList(column(COLUMN_NAME_2, Native.INT)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadataSecond).get();

    // send events to two independent topics
    RowUpdateEvent rowMutationEventFirstTopic =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            "col_value",
            column(COLUMN_NAME, Native.TEXT),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
            tableMetadataFirst,
            1000);
    kafkaCDCProducer.send(rowMutationEventFirstTopic).get();

    RowUpdateEvent rowMutationEventSecondTopic =
        createRowUpdateEvent(
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            123456,
            column(COLUMN_NAME_2, Native.INT),
            CLUSTERING_KEY_VALUE,
            clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
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

  private void mockOnePKOneCKOneColumn(TableMetadata tableMetadata) {
    mockOnePKOneCKAndColumns(
        tableMetadata, Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
  }

  private void mockOnePKOneCKAndColumns(
      TableMetadata tableMetadata, List<ColumnMetadata> columnMetadata) {
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns()).thenReturn(columnMetadata);
  }

  @SuppressWarnings("unchecked")
  public static Stream<Arguments> complexTypesProvider() {
    Collection listType = new Collection(Kind.LIST, Native.INT);
    Collection listOfSet = new Collection(Kind.LIST, new Collection(Kind.SET, Native.INT));
    Collection setType = new Collection(Kind.SET, Native.INT);
    Collection setOfList = new Collection(Kind.SET, new Collection(Kind.LIST, Native.INT));
    // udt
    LinkedHashMap<String, CQLType> udtColumns = new LinkedHashMap<>();
    udtColumns.put("udtcol_1", Native.INT);
    udtColumns.put("udtcol_2", Native.TEXT);
    UserDefined userDefinedType = new UserDefined("ks", "typeName", udtColumns);
    Map<String, Object> udtValue =
        ImmutableMap.<String, Object>builder().put("udtcol_1", 47).put("udtcol_2", "value").build();
    // nested udt
    LinkedHashMap<String, CQLType> nestedUdtColumns = new LinkedHashMap<>();
    nestedUdtColumns.put("nested", userDefinedType);
    nestedUdtColumns.put("list", new Collection(Kind.LIST, Native.INT));
    UserDefined userDefinedTypeNested = new UserDefined("ks", "nested", nestedUdtColumns);
    Map<String, Object> nestedUdtValue =
        ImmutableMap.<String, Object>builder()
            .put("nested", udtValue)
            .put("list", Arrays.asList(1, 2))
            .build();
    Custom customType = new Custom("java.className");

    // tuple
    Tuple tupleType = new Tuple(Native.INT, new Collection(Kind.LIST, Native.TEXT), Native.DECIMAL);
    List<Object> tupleValues =
        Arrays.asList(1, Collections.singletonList("v_1"), new BigDecimal(100));

    // nested tuple
    Tuple nestedTupleType = new Tuple(Native.INT, tupleType);
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
}
