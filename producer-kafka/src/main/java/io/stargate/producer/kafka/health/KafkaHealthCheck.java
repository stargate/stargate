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

import com.codahale.metrics.health.HealthCheck;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.config.ConfigResource;

public class KafkaHealthCheck extends HealthCheck {
  private final AdminClient adminClient;
  public static final String REPLICATION_PROPERTY = "transaction.state.log.replication.factor";
  public static final int DEFAULT_TIMEOUT_MILLIS = 5_000;
  private static final DescribeClusterOptions DESCRIBE_OPTIONS =
      new DescribeClusterOptions().timeoutMs(DEFAULT_TIMEOUT_MILLIS);

  public KafkaHealthCheck(Map<String, Object> kafkaSettings) {
    this.adminClient = AdminClient.create(kafkaSettings);
  }

  @Override
  protected Result check() throws Exception {
    DescribeClusterResult result = adminClient.describeCluster();
    String brokerId = result.controller().get().idString();
    int replicationFactor = getReplicationFactor(brokerId, adminClient);
    int nodes = result.nodes().get().size();
    ResultBuilder resultBuilder = Result.builder();
    if (nodes >= replicationFactor) {
      resultBuilder = resultBuilder.healthy().withMessage("Kafka cluster UP");
    } else {
      resultBuilder = resultBuilder.unhealthy().withMessage("Kafka cluster DOWN");
      ;
    }

    return resultBuilder
        .withDetail("clusterId", result.clusterId().get())
        .withDetail("brokerId", brokerId)
        .withDetail("nodes", nodes)
        .build();
  }

  private int getReplicationFactor(String brokerId, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
    Map<ConfigResource, Config> kafkaConfig =
        adminClient.describeConfigs(Collections.singletonList(configResource)).all().get();
    Config brokerConfig = kafkaConfig.get(configResource);
    return Integer.parseInt(brokerConfig.get(REPLICATION_PROPERTY).value());
  }
}
