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
package io.stargate.producer.kafka.schema;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import java.io.Closeable;
import java.util.Properties;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedSchemaRegistryServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedSchemaRegistryServer.class);

  private static final String DEFAULT_KAFKA_CONNECTION_URL = "localhost:2181";

  private Server server;
  private final String schemaRegistryUrl;
  private final String kafkaConnectionUrl;
  private final String bootstrapServers;

  public EmbeddedSchemaRegistryServer(
      String schemaRegistryUrl, String kafkaConnectionUrl, String bootstrapServers) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.kafkaConnectionUrl = kafkaConnectionUrl;
    this.bootstrapServers = bootstrapServers;
  }

  public void startSchemaRegistry() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.LISTENERS_CONFIG, schemaRegistryUrl);
    props.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, kafkaConnectionUrl);
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);

    try {
      server = app.createServer();
      server.start();
    } catch (Exception ex) {
      LOGGER.error("Error when starting schema registry", ex);
    }

    LOGGER.info("Schema Registry server started, listening for requests...");
  }

  @Override
  public void close() {
    try {
      stopServer();
    } catch (final Exception e) {
      LOGGER.error("Error shutdown embedded schema registry...", e);
      throw new RuntimeException("Error shutdown embedded schema registry...", e);
    }
  }

  void stopServer() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public String getKafkaConnectionUrl() {
    return kafkaConnectionUrl;
  }
}
