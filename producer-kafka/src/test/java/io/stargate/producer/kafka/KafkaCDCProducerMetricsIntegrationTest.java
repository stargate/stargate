package io.stargate.producer.kafka;

import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_ENABLED_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_INCLUDE_TAGS_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_NAME_SETTING_NAME;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.PARTITION_KEY_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;

public class KafkaCDCProducerMetricsIntegrationTest extends IntegrationTestBase {

  @Test
  public void shouldRegisterMetricsWhenMetricsAreEnabled() throws Exception {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    String topicName = creteTopicName(tableMetadata);

    MetricRegistry metricRegistry = new MetricRegistry();
    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(metricRegistry);
    Map<String, Object> metricsSettings = new HashMap<>();
    metricsSettings.put(METRICS_ENABLED_SETTING_NAME, true);
    metricsSettings.put(METRICS_INCLUDE_TAGS_SETTING_NAME, true);
    metricsSettings.put(METRICS_NAME_SETTING_NAME, "producer-prefix");

    Map<String, Object> properties = createKafkaProducerSettings(metricsSettings);
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
      validateThatWasSendToKafka(expectedKey, expectedValue, topicName);
      // it should have all kafka producer metrics registered
      // todo better validation
      assertThat(metricRegistry.getMetrics().size()).isGreaterThan(100);

    } finally {
      kafkaCDCProducer.close().get();
    }
  }
}
