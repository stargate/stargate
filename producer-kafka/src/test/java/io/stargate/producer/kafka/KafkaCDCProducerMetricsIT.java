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

import static io.stargate.producer.kafka.KafkaCDCProducer.CONFIG_STORE_MODULE_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_ENABLED_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_INCLUDE_TAGS_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_NAME_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.MetricsConfig.METRICS_NAME_DEFAULT;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.PARTITION_KEY_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class KafkaCDCProducerMetricsIT extends IntegrationTestBase {

  @Test
  public void shouldRegisterMetricsWhenMetricsAreEnabled() throws Exception {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    String kafkaMetricsPrefix = "producer-prefix";
    MetricRegistry metricRegistry = new MetricRegistry();
    Map<String, Object> metricsSettings = new HashMap<>();
    metricsSettings.put(METRICS_ENABLED_SETTING_NAME, true);
    metricsSettings.put(METRICS_INCLUDE_TAGS_SETTING_NAME, true);
    metricsSettings.put(METRICS_NAME_SETTING_NAME, kafkaMetricsPrefix);

    ConfigStore configStore = mockConfigStore(metricsSettings);
    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(metricRegistry, configStore);
    kafkaCDCProducer.init().get();

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
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            "col_value",
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
      // it should have all kafka producer metrics registered (more than 100 metrics)
      assertThat(countMetricsByPrefix(kafkaMetricsPrefix, metricRegistry)).isGreaterThan(100);
      // validate number of sent records
      assertThat(getMetricValue(metricRegistry, "record-send-total", topicName)).isEqualTo(1.0);
      Double outgoingBytesRate = getMetricValue(metricRegistry, "outgoing-byte-rate");
      assertThat(outgoingBytesRate).isGreaterThan(10);

      // when send additional kafka event
      kafkaCDCProducer.send(rowMutationEvent).get();
      verifyReceivedByKafka(expectedKey, expectedValue, topicName);

      // then validate that number of records send increased
      assertThat(getMetricValue(metricRegistry, "record-send-total", topicName)).isEqualTo(2.0);
      assertThat(getMetricValue(metricRegistry, "outgoing-byte-rate"))
          .isGreaterThan(outgoingBytesRate);

    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldNotRegisterMetricsWhenMetricsAreDisabled() throws Exception {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = createTopicName(tableMetadata);

    MetricRegistry metricRegistry = new MetricRegistry();
    Map<String, Object> metricsSettings = new HashMap<>();
    metricsSettings.put(METRICS_ENABLED_SETTING_NAME, false);
    ConfigStore configStore = mockConfigStore(metricsSettings);
    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(metricRegistry, configStore);

    kafkaCDCProducer.init().get();

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
            PARTITION_KEY_VALUE,
            partitionKey(PARTITION_KEY_NAME, Native.TEXT),
            "col_value",
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
      assertThat(countMetricsByPrefix(METRICS_NAME_DEFAULT, metricRegistry)).isEqualTo(0);

    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @NotNull
  private ConfigStore mockConfigStore(Map<String, Object> metricsSettings) {
    Map<String, Object> properties = createKafkaProducerSettings(metricsSettings);
    ConfigStore configStore = mock(ConfigStore.class);
    when(configStore.getConfigForModule(CONFIG_STORE_MODULE_NAME))
        .thenReturn(new ConfigWithOverrides(properties));
    return configStore;
  }

  private long countMetricsByPrefix(String kafkaMetricsPrefix, MetricRegistry metricRegistry) {
    return metricRegistry.getMetrics().keySet().stream()
        .filter(v -> v.startsWith(kafkaMetricsPrefix))
        .count();
  }

  private Double getMetricValue(MetricRegistry metricRegistry, String metricName) {
    return getMetricValue(metricRegistry, metricName, "");
  }

  private Double getMetricValue(
      MetricRegistry metricRegistry, String metricName, String topicName) {
    return (Double)
        metricRegistry.getGauges().entrySet().stream()
            .filter(v -> v.getKey().contains(metricName) && v.getKey().contains(topicName))
            .findFirst()
            .get()
            .getValue()
            .getValue();
  }
}
