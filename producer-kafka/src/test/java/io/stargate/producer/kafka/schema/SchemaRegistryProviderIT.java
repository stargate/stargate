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
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.SchemasTestConstants.PARTITION_KEY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.stargate.producer.kafka.IntegrationTestBase;
import io.stargate.producer.kafka.mapping.DefaultMappingService;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SchemaRegistryProviderIT extends IntegrationTestBase {

  @Test
  public void shouldAllowUpdatingTheSameSchema() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();

    // when update the same schema once again
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then should update without problems
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
  }

  @ParameterizedTest
  @MethodSource("newColumnsProvider")
  public void shouldAllowAddingNewColumnBecauseChangeIsBackwardCompatible(
      Consumer<TableMetadata> tableMetadataModification) {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when add a new column
    tableMetadataModification.accept(tableMetadata);
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then should retrieve new schema
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
  }

  @Test
  public void shouldNotAllowAddingNewPKBecauseChangeIsNotBackwardCompatible() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when add a new column
    when(tableMetadata.getPartitionKeys())
        .thenReturn(
            Arrays.asList(
                partitionKey(PARTITION_KEY_NAME, Native.TEXT), partitionKey("pk_2", Native.TEXT)));

    // then
    assertThatThrownBy(() -> schemaRegistryProvider.createOrUpdateSchema(tableMetadata))
        .hasRootCauseInstanceOf(RestClientException.class)
        .hasRootCauseMessage(
            "Schema being registered is incompatible with an earlier schema; error code: 409");
  }

  @ParameterizedTest
  @MethodSource("removeColumnsProvider")
  public void shouldAllowRemovingColumnBecauseChangeIsBackwardCompatible(
      Consumer<TableMetadata> tableMetadataModification) {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when remove a column
    tableMetadataModification.accept(tableMetadata);

    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then should retrieve new schema
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
  }

  @Test
  public void shouldNotAllowRemovingPKBecauseChangeIsNotBackwardCompatible() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when remove a column
    when(tableMetadata.getPartitionKeys()).thenReturn(Collections.emptyList());
    assertThatThrownBy(() -> schemaRegistryProvider.createOrUpdateSchema(tableMetadata))
        .hasRootCauseInstanceOf(RestClientException.class)
        .hasRootCauseMessage(
            "Schema being registered is incompatible with an earlier schema; error code: 409");
  }

  @ParameterizedTest
  @MethodSource("renameColumnsProvider")
  public void shouldAllowRenamingColumnBecauseChangeIsBackwardCompatible(
      Consumer<TableMetadata> tableMetadataModification) {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when remove a column
    tableMetadataModification.accept(tableMetadata);

    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // then should retrieve new schema
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
  }

  @Test
  public void shouldNotAllowRenamingPKBecauseChangeIsNotBackwardCompatible() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);

    // and when rename a PK
    when(tableMetadata.getPartitionKeys())
        .thenReturn(
            Collections.singletonList(partitionKey(PARTITION_KEY_NAME + "_renamed", Native.TEXT)));
    assertThatThrownBy(() -> schemaRegistryProvider.createOrUpdateSchema(tableMetadata))
        .hasRootCauseInstanceOf(RestClientException.class)
        .hasRootCauseMessage(
            "Schema being registered is incompatible with an earlier schema; error code: 409");
  }

  @Test
  public void shouldFetchTheLatestSchemaWhenThereIsNoTrackedIdButSchemaWasCreatedBefore() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // when
    schemaRegistryProvider.createOrUpdateSchema(tableMetadata);
    // and recreate to clear tracked schema ids
    schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);
    // does not have tracked schema id yet
    assertThat(schemaRegistryProvider.schemaIdPerSubject).hasSize(0);

    // then retrieve latest and don't track it's id
    assertThat(
            schemaRegistryProvider.getKeySchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(
            schemaRegistryProvider.getValueSchemaForTopic(
                mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isNotNull();
    assertThat(schemaRegistryProvider.schemaIdPerSubject).hasSize(0);
  }

  @Test
  public void shouldThrowWhenThereIsNoTrackedIdAndSchemaWasNotCreatedBefore() {
    // given
    TableMetadata tableMetadata = mockTableMetadata();
    DefaultMappingService mappingService = new DefaultMappingService(generatePrefix());
    SchemaRegistryProvider schemaRegistryProvider =
        new SchemaRegistryProvider(schemaRegistry.getSchemaRegistryUrl(), mappingService);

    // then
    assertThatThrownBy(
            () ->
                schemaRegistryProvider.getKeySchemaForTopic(
                    mappingService.getTopicNameFromTableMetadata(tableMetadata)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "The getSchemaBySubject was called before createOrUpdateSchema and there is no existing schema created for subject");
  }

  public static Stream<Arguments> newColumnsProvider() {
    return Stream.of(
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getColumns())
                      .thenReturn(
                          Arrays.asList(
                              column(COLUMN_NAME, Native.TEXT), column("col_2", Native.TEXT)));
                }), // new column
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getClusteringKeys())
                      .thenReturn(
                          Arrays.asList(
                              clusteringKey(CLUSTERING_KEY_NAME, Native.INT),
                              clusteringKey("CK_2", Native.TEXT)));
                })); // new clustering column
  }

  public static Stream<Arguments> removeColumnsProvider() {
    return Stream.of(
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getColumns()).thenReturn(Collections.emptyList());
                }), // remove column
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getClusteringKeys()).thenReturn(Collections.emptyList());
                })); // remove clustering column
  }

  public static Stream<Arguments> renameColumnsProvider() {
    return Stream.of(
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getColumns())
                      .thenReturn(
                          Collections.singletonList(column(COLUMN_NAME + "_renamed", Native.TEXT)));
                }), // rename column
        Arguments.of(
            (Consumer<TableMetadata>)
                tableMetadata -> {
                  when(tableMetadata.getClusteringKeys())
                      .thenReturn(
                          Collections.singletonList(
                              clusteringKey(CLUSTERING_KEY_NAME + "_renamed", Native.INT)));
                })); // rename clustering column
  }

  public TableMetadata mockTableMetadata() {
    TableMetadata tableMetadata = super.mockTableMetadata();
    when(tableMetadata.getPartitionKeys())
        .thenReturn(Collections.singletonList(partitionKey(PARTITION_KEY_NAME, Native.TEXT)));
    when(tableMetadata.getClusteringKeys())
        .thenReturn(Collections.singletonList(clusteringKey(CLUSTERING_KEY_NAME, Native.INT)));
    when(tableMetadata.getColumns())
        .thenReturn(Collections.singletonList(column(COLUMN_NAME, Native.TEXT)));
    return tableMetadata;
  }

  @NotNull
  private String generatePrefix() {
    return "prefix" + UUID.randomUUID().toString();
  }
}
