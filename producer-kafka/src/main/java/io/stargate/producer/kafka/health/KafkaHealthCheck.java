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
import com.google.common.annotations.VisibleForTesting;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.config.ConfigResource;

public class KafkaHealthCheck extends HealthCheck {
  public static final String REPLICATION_PROPERTY = "transaction.state.log.replication.factor";
  public static final int DEFAULT_TIMEOUT_MILLIS = 5_000; // default is 60 seconds
  private static final DescribeClusterOptions DESCRIBE_OPTIONS =
      new DescribeClusterOptions().timeoutMs(DEFAULT_TIMEOUT_MILLIS);
  private final Map<String, Object> kafkaSettings;

  /**
   * It requires only {@link
   * org.apache.kafka.clients.producer.ProducerConfig#BOOTSTRAP_SERVERS_CONFIG} setting
   */
  public KafkaHealthCheck(Map<String, Object> kafkaSettings) {
    this.kafkaSettings = kafkaSettings;
  }

  @Override
  protected Result check() {
    try (AdminClient adminClient = AdminClient.create(kafkaSettings)) {
      ResultBuilder resultBuilder = Result.builder();
      DescribeClusterResult result = adminClient.describeCluster(DESCRIBE_OPTIONS);
      try {
        Result errorMessage = validateIfDataWasReported(result);
        if (errorMessage != null) return errorMessage;

        String brokerId = result.controller().get().idString();
        int replicationFactor = getReplicationFactor(brokerId, adminClient);
        int nodes = result.nodes().get().size();
        if (nodes >= replicationFactor) {
          resultBuilder = resultBuilder.healthy().withMessage("Kafka cluster UP");
        } else {
          resultBuilder =
              resultBuilder.unhealthy().withMessage("Kafka cluster is under replicated");
        }
        return resultBuilder
            .withDetail("clusterId", result.clusterId().get())
            .withDetail("brokerId", brokerId)
            .withDetail("nodes", nodes)
            .build();
      } catch (InterruptedException | ExecutionException e) {
        return resultBuilder.unhealthy(e).withMessage("Kafka cluster DOWN").build();
      }
    }
  }

  @VisibleForTesting
  static Result validateIfDataWasReported(DescribeClusterResult result)
      throws InterruptedException, ExecutionException {
    boolean nodesNotEmpty = !result.nodes().get().isEmpty();
    boolean clusterIdAvailable = result.clusterId().get() != null;
    boolean aControllerExists = result.controller().get() != null;
    List<String> errors = new ArrayList<>();
    if (!nodesNotEmpty) {
      errors.add("no nodes found");
    }

    if (!clusterIdAvailable) {
      errors.add("no cluster id available");
    }

    if (!aControllerExists) {
      errors.add("no active controller exists");
    }

    if (!errors.isEmpty()) {
      String errorMessage = String.join(",", errors);
      return Result.unhealthy(errorMessage);
    }
    return null;
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
