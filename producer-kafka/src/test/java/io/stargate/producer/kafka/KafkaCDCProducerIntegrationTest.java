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
import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemasConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasConstants.PARTITION_KEY_NAME;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT;

import com.google.common.collect.Streams;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.stargate.producer.kafka.configuration.ConfigLoader;
import io.stargate.producer.kafka.schema.EmbeddedSchemaRegistryServer;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

class KafkaCDCProducerIntegrationTest {

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
    when(tableMetadata.getName()).thenReturn("tableName");
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
                return Streams.stream(records)
                    .anyMatch(r -> r.key().equals(expectedKey) && r.value().equals(expectedValue));
              });
    } finally {
      consumer.close();
    }
  }
}
