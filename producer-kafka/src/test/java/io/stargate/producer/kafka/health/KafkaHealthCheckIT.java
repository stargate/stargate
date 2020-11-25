package io.stargate.producer.kafka.health;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.codahale.metrics.health.HealthCheck.Result;
import io.stargate.producer.kafka.IntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.util.SocketUtils;

class KafkaHealthCheckIT extends IntegrationTestBase {

  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @AfterEach
  public void stopKafka() {
    if (embeddedKafkaBroker != null) {
      embeddedKafkaBroker.destroy();
    }
  }

  @Test
  public void shouldReportThatKafkaIsHealthy() throws Exception {
    // when
    startKafka(1);
    KafkaHealthCheck kafkaHealthCheck =
        new KafkaHealthCheck(createKafkaSettings(embeddedKafkaBroker.getBrokersAsString()));

    // then
    Result result = kafkaHealthCheck.check();
    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster UP");
    assertDetails(result.getDetails());
  }

  @Test
  public void shouldReportThatKafkaIsUnhealthy() throws Exception {
    // when
    int freePort = SocketUtils.findAvailableTcpPort();
    KafkaHealthCheck kafkaHealthCheck =
        new KafkaHealthCheck(createKafkaSettings("127.0.0.1:" + freePort));
    Result result = kafkaHealthCheck.check();

    // then
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster DOWN");
    assertDetails(result.getDetails());
  }

  @NotNull
  private Map<String, Object> createKafkaSettings(String brokers) {
    return Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
  }

  private void assertDetails(Map<String, Object> details) {
    assertThat(details).containsEntry("brokerId", "0");
    assertThat(details).containsKey("clusterId");
    assertThat(details).containsEntry("nodes", 1);
  }

  private void startKafka(int replicationFactor) throws Exception {
    embeddedKafkaBroker = new EmbeddedKafkaBroker(1);
    embeddedKafkaBroker.brokerProperties(
        Collections.singletonMap(
            KafkaHealthCheck.REPLICATION_PROPERTY, String.valueOf(replicationFactor)));
    embeddedKafkaBroker.afterPropertiesSet();
  }
}
