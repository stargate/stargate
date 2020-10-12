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

import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemaConstants.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.OPERATION_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.TIMESTAMP_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.VALUE_FIELD_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.stargate.producer.kafka.mapping.MappingService;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;

class SchemaRegistryProviderTest {

  @Test
  public void shouldConstructAvroSchemaForKey() {
    // given
    MappingService mappingService = mock(MappingService.class);
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(new MockSchemaRegistryClient(), mappingService);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn("topicName");
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Arrays.asList(partitionKey("f1", Native.TEXT), partitionKey("f2", Native.INT)));

    // when
    Schema schema = schemaRegistryProvider.constructKeySchema(tableMetadata);

    // then
    assertThat(schema.getNamespace()).isEqualTo("topicName");
    assertThat(schema.getName()).isEqualTo("Key");
    assertThat(schema.getField("f1").schema().getType()).isEqualTo(Type.STRING);
    assertThat(schema.getField("f2").schema().getType()).isEqualTo(Type.INT);
  }

  @Test
  public void shouldConstructAvroSchemaForValue() {
    // given
    MappingService mappingService = mock(MappingService.class);
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(new MockSchemaRegistryClient(), mappingService);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn("topicName");
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey("pk1", Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey("ck1", Native.TEXT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(clusteringKey("col1", Native.TEXT)));

    // when
    Schema schema = schemaRegistryProvider.constructValueSchema(tableMetadata);

    // then
    assertThat(schema.getNamespace()).isEqualTo("topicName");
    assertThat(schema.getName()).isEqualTo("Value");
    assertThat(schema.getField(OPERATION_FIELD_NAME).schema().getType()).isEqualTo(Type.STRING);
    assertThat(schema.getField(TIMESTAMP_FIELD_NAME).schema().getType()).isEqualTo(Type.LONG);
    assertThat(schema.getField(DATA_FIELD_NAME).schema().getType()).isEqualTo(Type.RECORD);
    isNullOrTypeUnion(schema, "pk1", Type.STRING);
    isNullOrTypeUnion(schema, "ck1", Type.STRING);
    isNullOrTypeUnion(schema, "col1", Type.UNION);
  }

  @Test
  public void shouldCreateAndRegisterKeyAndValueSchema() {
    // given
    String topicName = "topicName";
    MappingService mappingService = mock(MappingService.class);
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(new MockSchemaRegistryClient(), mappingService);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn(topicName);
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey("pk1", Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey("ck1", Native.TEXT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(clusteringKey("col1", Native.TEXT)));

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then
    Schema keySchemaForTopic = schemaRegistryProvider.getKeySchemaForTopic(topicName);
    Schema valueSchemaForTopic = schemaRegistryProvider.getValueSchemaForTopic(topicName);
    assertThat(keySchemaForTopic).isNotNull();
    assertThat(valueSchemaForTopic).isNotNull();
  }

  private void isNullOrTypeUnion(Schema schema, String columnName, Type expectedInnerType) {
    Schema dateFieldSchema = schema.getField(DATA_FIELD_NAME).schema();
    Schema columnSchema = dateFieldSchema.getField(columnName).schema();
    assertThat(columnSchema.getType()).isEqualTo(Type.UNION);
    assertThat(columnSchema.getTypes().get(0).getType()).isEqualTo(Type.NULL);
    assertThat(columnSchema.getTypes().get(1).getType()).isEqualTo(Type.RECORD);
    assertThat(columnSchema.getTypes().get(1).getField(VALUE_FIELD_NAME).schema().getType())
        .isEqualTo(expectedInnerType);
  }
}
