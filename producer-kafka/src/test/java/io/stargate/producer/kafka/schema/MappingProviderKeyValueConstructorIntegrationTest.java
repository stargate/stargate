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
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.stargate.producer.kafka.mapping.MappingService;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;

public class MappingProviderKeyValueConstructorIntegrationTest {
  @Test
  public void shouldWriteKeyAndValueUsingDynamicallyCreatedSchema() {
    // given
    String topicName = "topic-1";
    MappingService mappingService = mock(MappingService.class);
    when(mappingService.getTopicNameFromTableMetadata(any())).thenReturn(topicName);
    SchemaProvider schemaProvider =
        new SchemaRegistryProvider(new MockSchemaRegistryClient(), mappingService);
    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn(topicName);
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey("pk1", Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey("ck1", Native.TEXT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(clusteringKey("col1", Native.TEXT)));

    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 100;
    MutationEvent rowMutationEvent =
        createRowUpdateEvent(
            partitionKeyValue,
            partitionKey("pk1", Native.TEXT),
            "col_value",
            column("col1"),
            clusteringKeyValue,
            clusteringKey("ck1", Native.INT),
            tableMetadata);

    // when
    // create schema
    schemaProvider.createOrUpdateSchema(tableMetadata);
    // and construct key using this schema
    GenericRecord key = keyValueConstructor.constructKey(rowMutationEvent, topicName);
    GenericRecord value = keyValueConstructor.constructValue(rowMutationEvent, topicName);

    // then should construct the key and value without problems
    assertThat(key).isNotNull();
    assertThat(value).isNotNull();
  }
}
