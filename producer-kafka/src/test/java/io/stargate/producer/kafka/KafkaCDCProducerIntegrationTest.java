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

import static io.stargate.producer.kafka.configuration.ConfigLoader.CDC_TOPIC_PREFIX_NAME;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.cell;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.cellValue;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemaConstants.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemasConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasConstants.COLUMN_NAME_2;
import static io.stargate.producer.kafka.schema.SchemasConstants.PARTITION_KEY_NAME;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.stargate.producer.kafka.configuration.ConfigLoader;
import io.stargate.producer.kafka.schema.EmbeddedSchemaRegistryServer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Collection.Kind;
import org.apache.cassandra.stargate.schema.CQLType.MapDataType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

class KafkaCDCProducerIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCDCProducerIntegrationTest.class);

  private static KafkaContainer kafkaContainer;
  private static EmbeddedSchemaRegistryServer schemaRegistry;

  private static final String TOPIC_PREFIX = "topicPrefix";

  @BeforeAll
  public static void setup() throws Exception {
    Network network = Network.newNetwork();
    kafkaContainer = new KafkaContainer().withNetwork(network).withEmbeddedZookeeper();
    kafkaContainer.start();
    try (ServerSocket serverSocket = new ServerSocket(0)) {

      schemaRegistry =
          new EmbeddedSchemaRegistryServer(
              String.format("http://localhost:%s", serverSocket.getLocalPort()),
              String.format("localhost:%s", ZOOKEEPER_PORT),
              kafkaContainer.getBootstrapServers());
    }
    schemaRegistry.startSchemaRegistry();
  }

  @AfterAll
  public static void cleanup() {
    kafkaContainer.stop();
    schemaRegistry.close();
  }

  @Test
  public void shouldSendUpdateEventWithOnePartitionKeyAndOneValue() throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    String columnValue = "col_value";
    long timestamp = 1000;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();

    // send actual event
    RowUpdateEvent rowMutationEvent =
        createRowUpdateEvent(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            columnValue,
            column(COLUMN_NAME, Native.TEXT),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
            tableMetadata,
            timestamp);
    kafkaCDCProducer.send(rowMutationEvent).get();

    // then
    GenericRecord expectedKey =
        kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
    GenericRecord expectedValue =
        kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

    try {
      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  private String creteTopicName(TableMetadata tableMetadata) {
    return String.format(
        "%s.%s.%s", TOPIC_PREFIX, tableMetadata.getKeyspace(), tableMetadata.getName());
  }

  private TableMetadata mockTableMetadata() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getKeyspace()).thenReturn("keyspaceName");
    when(tableMetadata.getName()).thenReturn("tableName" + UUID.randomUUID().toString());
    return tableMetadata;
  }

  @Test
  public void shouldSendDeleteEventForAllPKsAndCK() throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    long timestamp = 1234;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata);

    // end delete event
    DeleteEvent event =
        createDeleteEvent(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            tableMetadata,
            timestamp);
    kafkaCDCProducer.send(event).get();

    // then
    GenericRecord expectedKey = kafkaCDCProducer.keyValueConstructor.constructKey(event, topicName);
    GenericRecord expectedValue =
        kafkaCDCProducer.keyValueConstructor.constructValue(event, topicName);

    try {
      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @ParameterizedTest
  @MethodSource("columnsAfterChange")
  public void shouldSendUpdateAndSendSecondEventWhenSchemaChanged(
      List<ColumnMetadata> metadataAfterChange, List<Cell> columnsAfterChange) throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    String columnValue = "col_value";
    long timestamp = 1000;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              partitionKeyValue,
              partitionKey(PARTITION_KEY_NAME, Native.TEXT),
              columnValue,
              column(COLUMN_NAME, Native.TEXT),
              clusteringKeyValue,
              clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
              tableMetadata,
              timestamp);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);

      // when change schema
      when(tableMetadata.getPartitionKeys())
          .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
      when(tableMetadata.getClusteringKeys())
          .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
      when(tableMetadata.getColumns()).thenReturn(metadataAfterChange);
      kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();

      // and send event with a new column
      rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(partitionKeyValue, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnsAfterChange,
              Collections.singletonList(
                  cellValue(clusteringKeyValue, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
              tableMetadata,
              timestamp);

      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      expectedKey = kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);

    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @ParameterizedTest
  @MethodSource("nativeTypesProvider")
  public void shouldSendEventsWithAllNativeTypes(
      List<ColumnMetadata> columnMetadata, List<Cell> columnValues) throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    long timestamp = 1000;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns()).thenReturn(columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(partitionKeyValue, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnValues,
              Collections.singletonList(
                  cellValue(clusteringKeyValue, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
              tableMetadata,
              timestamp);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
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
  public void shouldSendEventsWithAllComplexTypes(
      List<ColumnMetadata> columnMetadata, List<Cell> columnValues) throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    long timestamp = 1000;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    // schema change event
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns()).thenReturn(columnMetadata);
    kafkaCDCProducer.createTableSchemaAsync(tableMetadata).get();
    try {
      // send actual event
      RowUpdateEvent rowMutationEvent =
          createRowUpdateEvent(
              Collections.singletonList(
                  cellValue(partitionKeyValue, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              columnValues,
              Collections.singletonList(
                  cellValue(clusteringKeyValue, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
              tableMetadata,
              timestamp);
      kafkaCDCProducer.send(rowMutationEvent).get();

      // then
      GenericRecord expectedKey =
          kafkaCDCProducer.keyValueConstructor.constructKey(rowMutationEvent, topicName);
      GenericRecord expectedValue =
          kafkaCDCProducer.keyValueConstructor.constructValue(rowMutationEvent, topicName);

      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldSendEventsWithMapAndNestedMap() throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    long timestamp = 1000;
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer();
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
                  cellValue(partitionKeyValue, partitionKey(PARTITION_KEY_NAME, Native.TEXT))),
              Arrays.asList(
                  cell(column(COLUMN_NAME, mapType), mapValues),
                  cell(column(COLUMN_NAME_2, mapOfMapType), mapOfMapValues)),
              Collections.singletonList(
                  cellValue(clusteringKeyValue, clusteringKey(CLUSTERING_KEY_NAME, Native.INT))),
              tableMetadata,
              timestamp);
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

      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @SuppressWarnings("unchecked")
  public static Stream<Arguments> complexTypesProvider() {
    Collection listType = new Collection(Kind.LIST, Native.INT);
    Collection listOfSet = new Collection(Kind.LIST, new Collection(Kind.SET, Native.INT));
    Collection setType = new Collection(Kind.SET, Native.INT);
    Collection setOfList = new Collection(Kind.SET, new Collection(Kind.LIST, Native.INT));

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
                    Sets.newHashSet(Collections.singletonList(1), Collections.singletonList(2))))));
  }

  @NotNull
  private Map<String, Object> createKafkaProducerSettings() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CDC_TOPIC_PREFIX_NAME, TOPIC_PREFIX);

    properties.put(
        withCDCPrefixPrefix(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
        kafkaContainer.getBootstrapServers());
    properties.put(
        withCDCPrefixPrefix(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), KafkaAvroSerializer.class);
    properties.put(
        withCDCPrefixPrefix(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
        KafkaAvroSerializer.class);
    // lower the max.block to allow faster failure scenario testing
    properties.put(withCDCPrefixPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), "2000");

    properties.put(
        withCDCPrefixPrefix("schema.registry.url"), schemaRegistry.getSchemaRegistryUrl());
    return properties;
  }

  @NotNull
  private String withCDCPrefixPrefix(String settingName) {
    return String.format("%s.%s", ConfigLoader.CDC_KAFKA_PRODUCER_SETTING_PREFIX, settingName);
  }

  @SuppressWarnings("UnstableApiUsage")
  private void validateThatWasSendToKafka(
      GenericRecord expectedKey, GenericRecord expectedValue, String topicName) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());

    KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topicName));

    try {
      await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () -> {
                ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                  LOG.info(
                      "Retrieved {} records: {}",
                      records.count(),
                      IteratorUtils.toList(records.iterator()));
                }
                return Streams.stream(records)
                    .anyMatch(
                        r -> {
                          System.out.println("expectedValue:" + expectedValue);
                          System.out.println("actualValue:" + r.value());

                          return r.key().equals(expectedKey) && r.value().equals(expectedValue);
                        });
              });
    } finally {
      consumer.close();
    }
  }
}
