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
package io.stargate.producer.kafka.health;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck.Result;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.util.SocketUtils;

class KafkaHealthCheckIT {

  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @AfterEach
  public void stopKafka() {
    if (embeddedKafkaBroker != null) {
      embeddedKafkaBroker.destroy();
    }
  }

  @Test
  public void shouldReportThatKafkaIsHealthy() {
    // given
    startKafka(1);

    // when
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
    KafkaHealthCheck kafkaHealthCheck =
        new KafkaHealthCheck(
            createKafkaSettings("127.0.0.1:" + SocketUtils.findAvailableTcpPort()));
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
    // given
    startKafka(2);

    // when
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
    Map<String, Object> kafkaSettings =
        createKafkaSettings("127.0.0.1:" + SocketUtils.findAvailableTcpPort());
    KafkaHealthCheck kafkaHealthCheck = new KafkaHealthCheck(kafkaSettings);

    // then
    Result result = kafkaHealthCheck.check();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Kafka cluster DOWN");

    // when start kafka
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

  @ParameterizedTest
  @MethodSource("clusterInfoProvider")
  public void shouldReturnUnhealthyWhenRequiredDataIsNotPresent(
      Collection<Node> nodes, Node controller, String clusterId, String errorMessage)
      throws ExecutionException, InterruptedException {
    // given
    DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture(clusterId));
    when(describeClusterResult.controller()).thenReturn(KafkaFuture.completedFuture(controller));
    when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));

    // when
    Result result = KafkaHealthCheck.validateIfDataWasReported(describeClusterResult);

    // then
    assertThat(result).isNotNull();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo(errorMessage);
  }

  @Test
  public void shouldReturnNullWhenRequiredDataIsPresent()
      throws ExecutionException, InterruptedException {
    // given
    DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture("cluster_id"));
    when(describeClusterResult.controller())
        .thenReturn(KafkaFuture.completedFuture(mock(Node.class)));
    when(describeClusterResult.nodes())
        .thenReturn(KafkaFuture.completedFuture(Collections.singleton(mock(Node.class))));

    // when
    Result result = KafkaHealthCheck.validateIfDataWasReported(describeClusterResult);

    // then
    assertThat(result).isNull();
  }

  public static Stream<Arguments> clusterInfoProvider() {
    Collection<Node> nodesInCluster = Collections.singleton(mock(Node.class));
    Node controller = mock(Node.class);

    return Stream.of(
        Arguments.of(nodesInCluster, controller, null, "no cluster id available"),
        Arguments.of(nodesInCluster, null, "cluster_id", "no active controller exists"),
        Arguments.of(Collections.emptyList(), controller, "cluster_id", "no nodes found"),
        Arguments.of(
            nodesInCluster, null, null, "no cluster id available,no active controller exists"),
        Arguments.of(
            Collections.emptyList(), controller, null, "no nodes found,no cluster id available"),
        Arguments.of(
            Collections.emptyList(),
            null,
            "cluster_id",
            "no nodes found,no active controller exists"),
        Arguments.of(
            Collections.emptyList(),
            null,
            null,
            "no nodes found,no cluster id available,no active controller exists"));
  }

  @NotNull
  private static Map<String, Object> createKafkaSettings(String brokers) {
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
