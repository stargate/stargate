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

import static io.stargate.producer.kafka.schema.SchemaConstants.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.OPERATION_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.TIMESTAMP_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.TIMESTAMP_MILLIS_TYPE;
import static io.stargate.producer.kafka.schema.SchemaConstants.VALUE_FIELD_NAME;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.stargate.producer.kafka.mapping.MappingService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
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
  private int valueSchemaId;

  private MappingService mappingService;

  public SchemaRegistryProvider(String schemaRegistryUrl, MappingService mappingService) {
    schemaRegistryClient =
        new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_REGISTRY_MAX_CAPACITY);
    this.mappingService = mappingService;
  }

  @Override
  public Schema getKeySchemaForTopic(String topicName) {
    String subjectName = constructKeyRecordName(topicName);
    return getSchemaBySubjectAndId(subjectName, keySchemaId);
  }

  @Override
  public Schema getValueSchemaForTopic(String topicName) {
    String subjectName = constructValueRecordName(topicName);
    return getSchemaBySubjectAndId(subjectName, valueSchemaId);
  }

  @Override
  public void createOrUpdateSchema(TableMetadata tableMetadata) {
    createOrUpdateKeySchema(tableMetadata);
    createOrUpdateValueSchema(tableMetadata);
  }

  private Schema getSchemaBySubjectAndId(String subjectName, int keySchemaId) {
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

  private void createOrUpdateValueSchema(TableMetadata tableMetadata) {
    ParsedSchema valueSchema = new AvroSchema(constructValueSchema(tableMetadata));
    String subject = constructValueRecordName(tableMetadata);

    valueSchemaId = registerSchema(tableMetadata, valueSchema, subject);

    logger.info(
        "Registered valueSchema: {}, for subject: {} and id: {}",
        valueSchema,
        subject,
        valueSchemaId);
  }

  private void createOrUpdateKeySchema(TableMetadata tableMetadata) {
    ParsedSchema keySchema = new AvroSchema(constructKeySchema(tableMetadata));
    String subject = constructKeyRecordName(tableMetadata);

    keySchemaId = registerSchema(tableMetadata, keySchema, subject);

    logger.info(
        "Registered keySchema: {}, for subject: {} and id: {}", keySchema, subject, keySchemaId);
  }

  private int registerSchema(TableMetadata tableMetadata, ParsedSchema keySchema, String subject) {
    try {
      return schemaRegistryClient.register(subject, keySchema);
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
    List<Schema> partitionKeys =
        constructRequiredValueFieldsSchema(tableMetadata.getPartitionKeys());
    List<Schema> clusteringKeys =
        constructRequiredValueFieldsSchema(tableMetadata.getClusteringKeys());
    List<Schema> columns = constructOptionalValueFieldsSchema(tableMetadata.getColumns());
    ;
    Schema fieldsSchema =
        constructFieldsSchema(partitionKeys, clusteringKeys, columns, tableMetadata);

    String valueRecordName = constructValueRecordName(tableMetadata);
    return SchemaBuilder.record(valueRecordName)
        .namespace(SCHEMA_NAMESPACE)
        .fields()
        .requiredString(OPERATION_FIELD_NAME)
        .name(TIMESTAMP_FIELD_NAME)
        .type(TIMESTAMP_MILLIS_TYPE)
        .noDefault()
        .name(DATA_FIELD_NAME)
        .type(fieldsSchema)
        .noDefault()
        .endRecord();
  }

  private Schema constructFieldsSchema(
      List<Schema> partitionKeys,
      List<Schema> clusteringKeys,
      List<Schema> columns,
      TableMetadata tableMetadata) {
    String dataRecordName = constructDataRecordName(tableMetadata);
    FieldAssembler<Schema> fields = SchemaBuilder.record(dataRecordName).fields();
    addToFields(partitionKeys, fields);
    addToFields(clusteringKeys, fields);
    addToFields(columns, fields);
    return fields.endRecord();
  }

  private void addToFields(List<Schema> fieldsToAdd, FieldAssembler<Schema> fields) {
    for (Schema schema : fieldsToAdd) {
      List<Field> fieldsForSpecificSchema = schema.getFields();
      if (fieldsForSpecificSchema.size() != 1) {
        throw new IllegalStateException(
            "The schema: " + schema + " must have one field, but has: " + fieldsForSpecificSchema);
      }
      Field fieldToAdd = fieldsForSpecificSchema.get(0);
      fields.name(fieldToAdd.name()).type(fieldToAdd.schema()).noDefault();
    }
  }

  private List<Schema> constructRequiredValueFieldsSchema(List<ColumnMetadata> tableMetadata) {
    List<Schema> partitionKeys = new ArrayList<>();
    for (ColumnMetadata columnMetadata : tableMetadata) {
      Schema partitionKey =
          SchemaBuilder.record(columnMetadata.getName())
              .fields()
              .name(VALUE_FIELD_NAME)
              .type(CqlToAvroTypeConverter.toAvroType(columnMetadata.getType()))
              .noDefault()
              .endRecord();
      partitionKeys.add(SchemaBuilder.unionOf().nullType().and().type(partitionKey).endUnion());
    }
    return partitionKeys;
  }

  private List<Schema> constructOptionalValueFieldsSchema(List<ColumnMetadata> tableMetadata) {
    List<Schema> partitionKeys = new ArrayList<>();
    for (ColumnMetadata columnMetadata : tableMetadata) {
      Schema partitionKey =
          SchemaBuilder.record(columnMetadata.getName())
              .fields()
              .name(VALUE_FIELD_NAME)
              .type()
              .optional()
              .type(CqlToAvroTypeConverter.toAvroType(columnMetadata.getType()))
              .endRecord();
      partitionKeys.add(SchemaBuilder.unionOf().nullType().and().type(partitionKey).endUnion());
    }
    return partitionKeys;
  }

  private String constructKeyRecordName(String topicName) {
    return String.format("%s.Key", topicName);
  }

  private String constructKeyRecordName(TableMetadata tableMetadata) {
    return constructKeyRecordName(mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }

  private String constructValueRecordName(TableMetadata tableMetadata) {
    return constructKeyRecordName(mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }

  private String constructValueRecordName(String topicName) {
    return String.format("%s.Value", topicName);
  }

  private String constructDataRecordName(TableMetadata tableMetadata) {
    return String.format("%s.Data", mappingService.getTopicNameFromTableMetadata(tableMetadata));
  }
}
