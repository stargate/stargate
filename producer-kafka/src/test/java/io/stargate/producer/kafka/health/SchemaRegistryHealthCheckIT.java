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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.codahale.metrics.health.HealthCheck;
import io.stargate.producer.kafka.schema.EmbeddedSchemaRegistryServer;
import java.net.ConnectException;
import java.net.ServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.util.SocketUtils;

class SchemaRegistryHealthCheckIT {

  private EmbeddedSchemaRegistryServer schemaRegistry;
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @AfterEach
  public void stopSchemaRegistry() {
    if (embeddedKafkaBroker != null) {
      embeddedKafkaBroker.destroy();
    }

    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
  }

  @Test
  public void shouldReportThatSchemaRegistryIsHealthy() throws Exception {
    // given
    startKafkaAndCreateSchemaRegistry();
    schemaRegistry.startSchemaRegistry();

    // when
    SchemaRegistryHealthCheck schemaRegistryHealthCheck =
        new SchemaRegistryHealthCheck(schemaRegistry.getSchemaRegistryUrl());

    // then
    HealthCheck.Result result = schemaRegistryHealthCheck.check();
    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Schema Registry is UP");
  }

  @Test
  public void shouldReportThatSchemaRegistryIsUnhealthy() {
    // when
    SchemaRegistryHealthCheck schemaRegistryHealthCheck =
        new SchemaRegistryHealthCheck("http://127.0.0.1:" + SocketUtils.findAvailableTcpPort());

    // then
    HealthCheck.Result result = schemaRegistryHealthCheck.check();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Schema Registry is DOWN");
    assertThat(result.getError()).isInstanceOf(ConnectException.class);
  }

  @Test
  public void shouldReportThatSchemaRegistryIsUnhealthyAndTransitionToHealthyWhenStarted()
      throws Exception {
    // given
    startKafkaAndCreateSchemaRegistry();

    // when
    SchemaRegistryHealthCheck schemaRegistryHealthCheck =
        new SchemaRegistryHealthCheck(schemaRegistry.getSchemaRegistryUrl());

    // then
    HealthCheck.Result result = schemaRegistryHealthCheck.check();
    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Schema Registry is DOWN");
    assertThat(result.getError()).isInstanceOf(ConnectException.class);

    // when start schema-registry
    schemaRegistry.startSchemaRegistry();

    // then report as up
    result = schemaRegistryHealthCheck.check();
    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Schema Registry is UP");
  }

  private void startKafkaAndCreateSchemaRegistry() throws Exception {
    embeddedKafkaBroker = new EmbeddedKafkaBroker(1);
    embeddedKafkaBroker.afterPropertiesSet(); // it starts the kafka broker

    try (ServerSocket serverSocket = new ServerSocket(0)) {

      schemaRegistry =
          new EmbeddedSchemaRegistryServer(
              String.format("http://localhost:%s", serverSocket.getLocalPort()),
              String.format("localhost:%s", embeddedKafkaBroker.getZkPort()),
              embeddedKafkaBroker.getBrokersAsString());
    }
  }
}
