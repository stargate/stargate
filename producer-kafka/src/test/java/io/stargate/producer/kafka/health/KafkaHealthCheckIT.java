package io.stargate.producer.kafka.health;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.codahale.metrics.health.HealthCheck.Result;
import io.stargate.producer.kafka.IntegrationTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
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
  public void shouldReportThatKafkaIsHealthy() {
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
  public void shouldReportThatKafkaIsUnhealthyWhenBrokerNonReachable() {
    // when
    int freePort = SocketUtils.findAvailableTcpPort();
    KafkaHealthCheck kafkaHealthCheck =
        new KafkaHealthCheck(createKafkaSettings("127.0.0.1:" + freePort));
    Result result = kafkaHealthCheck.check();

    // then
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster DOWN");
    assertThat(result.getError())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void shouldReportThatKafkaIsUnhealthyWhenNotEnoughReplicas() {
    // when
    startKafka(2);
    KafkaHealthCheck kafkaHealthCheck =
        new KafkaHealthCheck(createKafkaSettings(embeddedKafkaBroker.getBrokersAsString()));

    // then
    Result result = kafkaHealthCheck.check();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster is under replicated");
    assertDetails(result.getDetails());
  }

  @Test
  public void shouldReportThatKafkaIsUnhealthyAndTransitionToHealthyWhenBrokerStarted() {
    // when
    int freePort = SocketUtils.findAvailableTcpPort();
    Map<String, Object> kafkaSettings = createKafkaSettings("127.0.0.1:" + freePort);
    KafkaHealthCheck kafkaHealthCheck = new KafkaHealthCheck(kafkaSettings);

    // then
    Result result = kafkaHealthCheck.check();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster DOWN");

    // when start kafka again
    startKafka(1);
    // replace the broker with a new live-broker url
    kafkaSettings.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());

    // then report as up
    result = kafkaHealthCheck.check();
    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster UP");
    assertDetails(result.getDetails());
  }

  @NotNull
  private Map<String, Object> createKafkaSettings(String brokers) {
    HashMap<String, Object> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    return map;
  }

  private void assertDetails(Map<String, Object> details) {
    assertThat(details).containsEntry("brokerId", "0");
    assertThat(details).containsKey("clusterId");
    assertThat(details).containsEntry("nodes", 1);
  }

  private void startKafka(int replicationFactor) {
    embeddedKafkaBroker = new EmbeddedKafkaBroker(1);
    embeddedKafkaBroker.brokerProperties(
        Collections.singletonMap(
            KafkaHealthCheck.REPLICATION_PROPERTY, String.valueOf(replicationFactor)));
    embeddedKafkaBroker.afterPropertiesSet();
  }
}
