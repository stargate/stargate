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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.stargate.producer.kafka.mapping.MappingService;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegistryProvider implements SchemaProvider {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryProvider.class);

  private static final int SCHEMA_REGISTRY_MAX_CAPACITY = 1000;
  private final SchemaRegistryClient schemaRegistryClient;
  public static final String SCHEMA_NAMESPACE = "io.stargate.producer.kafka";
  private int keySchemaId;

  private MappingService mappingService;

  public SchemaRegistryProvider(String schemaRegistryUrl, MappingService mappingService) {
    schemaRegistryClient =
        new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_REGISTRY_MAX_CAPACITY);
    this.mappingService = mappingService;
  }

  @Override
  public Schema getKeySchemaForTopic(String topicName) {
    String subjectName = constructKeyRecordName(topicName);
    try {
      // todo assert that cast is safe
      return ((Schema) schemaRegistryClient.getSchemaBySubjectAndId(subjectName, keySchemaId));
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(
          "Problem when get key schema for subject: "
              + subjectName
              + " and schema id: "
              + keySchemaId,
          e);
    }
  }

  @Override
  public Schema getValueSchemaForTopic(String topicName) {
    return null;
  }

  @Override
  public void createOrUpdateSchema(TableMetadata tableMetadata) {
    createOrUpdateKeySchema(tableMetadata);
    createOrUpdateValueSchema(tableMetadata);
  }

  private void createOrUpdateValueSchema(TableMetadata tableMetadata) {
    ParsedSchema valueSchema = new AvroSchema(constructValueSchema(tableMetadata));
    String subject = mappingService.getTopicNameFromTableMetadata(tableMetadata);

    registerSchema(tableMetadata, valueSchema, subject);

    logger.info(
        "Registered valueSchema: {}, for subject: {} and id: {}",
        valueSchema,
        subject,
        keySchemaId);
  }

  private void createOrUpdateKeySchema(TableMetadata tableMetadata) {
    ParsedSchema keySchema = new AvroSchema(constructKeySchema(tableMetadata));
    String subject = constructKeyRecordName(tableMetadata);

    registerSchema(tableMetadata, keySchema, subject);

    logger.info(
        "Registered keySchema: {}, for subject: {} and id: {}", keySchema, subject, keySchemaId);
  }

  private void registerSchema(TableMetadata tableMetadata, ParsedSchema keySchema, String subject) {
    try {
      keySchemaId = schemaRegistryClient.register(subject, keySchema);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(
          "Problem when create or update key schema for tableMetadata: " + tableMetadata, e);
    }
  }

  private Schema constructKeySchema(TableMetadata tableMetadata) {
    String keyRecordName = constructKeyRecordName(tableMetadata);
    FieldAssembler<Schema> keyBuilder =
        SchemaBuilder.record(keyRecordName).namespace(SCHEMA_NAMESPACE).fields();

    for (ColumnMetadata columnMetadata : tableMetadata.getPartitionKeys()) {
      Schema avroFieldSchema = CqlToAvroTypeConverter.toAvroType(columnMetadata.getType());
      keyBuilder.name(columnMetadata.getName()).type(avroFieldSchema).noDefault();
    }
    return keyBuilder.endRecord();
  }

  private Schema constructValueSchema(TableMetadata tableMetadata) {
    Schema partitionKeys = constructPartitionKeysSchema(tableMetadata);
    return null;
  }

  private Schema constructPartitionKeysSchema(TableMetadata tableMetadata) {
    // todo
    return null;
  }

  private String constructKeyRecordName(String topicName) {
    return String.format("%s.Key", topicName);
  }

  private String constructKeyRecordName(TableMetadata tableMetadata) {
    return constructKeyRecordName(mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }

  private String constructValueRecordName(TableMetadata tableMetadata) {
    return String.format("%s.Value", mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }

  private String constructDataRecordName(TableMetadata tableMetadata) {
    return String.format("%s.Data", mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }
}
