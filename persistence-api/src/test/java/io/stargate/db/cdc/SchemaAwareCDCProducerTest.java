/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.cdc;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.cdc.api.DefaultMutationEvent;
import io.stargate.db.cdc.api.MutationEvent;
import io.stargate.db.cdc.api.MutationEventType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Table;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaAwareCDCProducerTest {
  @Test
  public void shouldUpdateSchemaWhenPublishNewTable() {
    // given
    MockSchemaAwareCDCProducer producer = new MockSchemaAwareCDCProducer();
    Table table = createTable();

    // when
    producer.publish(createMutationEvent(table));

    // then
    assertThat(producer.createNumberOfCalls).isEqualTo(1);
    assertThat(producer.sendNumberOfCalls).isEqualTo(1);
  }

  @Test
  public void shouldCallCreateTableSchemaOnceForSameTableCalls() {
    // given
    MockSchemaAwareCDCProducer producer = new MockSchemaAwareCDCProducer();
    Table table = createTable();

    // when
    producer.publish(createMutationEvent(table));
    producer.publish(createMutationEvent(table));

    // then
    assertThat(producer.createNumberOfCalls).isEqualTo(1);
    assertThat(producer.sendNumberOfCalls).isEqualTo(2);
  }

  @ParameterizedTest
  @MethodSource("tablesProvider")
  public void shouldCallCreateSchemaTwiceIfTwoTablesAreDifferent(
      Table baseTable, Table afterChange) {
    // given
    MockSchemaAwareCDCProducer producer = new MockSchemaAwareCDCProducer();

    // when
    producer.publish(createMutationEvent(baseTable));
    producer.publish(createMutationEvent(afterChange));

    // then
    assertThat(producer.createNumberOfCalls).isEqualTo(2);
    assertThat(producer.sendNumberOfCalls).isEqualTo(2);
  }

  public static Stream<Arguments> tablesProvider() {
    Table baseTable =
        ImmutableTable.builder()
            .keyspace("ks_1")
            .name("table_1")
            .addColumns(Column.create("col_1", Column.Kind.PartitionKey))
            .build();
    Table differentKeyspace =
        ImmutableTable.builder()
            .keyspace("ks_2")
            .name("table_1")
            .addColumns(Column.create("col_1", Column.Kind.PartitionKey))
            .build();
    Table differentTableName =
        ImmutableTable.builder()
            .keyspace("ks_1")
            .name("table_2")
            .addColumns(Column.create("col_1", Column.Kind.PartitionKey))
            .build();

    Table differentColumn =
        ImmutableTable.builder()
            .keyspace("ks_1")
            .name("table_1")
            .addColumns(Column.create("col_1", Column.Kind.Clustering))
            .build();

    return Stream.of(
        arguments(baseTable, differentKeyspace),
        arguments(baseTable, differentTableName),
        arguments(baseTable, differentColumn));
  }

  private ImmutableTable createTable() {
    return ImmutableTable.builder()
        .keyspace("ks_1")
        .name("table_1")
        .addColumns(Column.create("col_1", Column.Kind.PartitionKey))
        .build();
  }

  static class MockSchemaAwareCDCProducer extends SchemaAwareCDCProducer {
    private static final CompletableFuture<Void> COMPLETED_FUTURE =
        CompletableFuture.completedFuture(null);
    int initNumberOfCalls = 0;
    int createNumberOfCalls = 0;
    int sendNumberOfCalls = 0;
    int closeNumberOfCalls = 0;

    @Override
    public CompletableFuture<Void> init() {
      initNumberOfCalls += 1;
      return COMPLETED_FUTURE;
    }

    @Override
    protected CompletableFuture<Void> createTableSchemaAsync(Table table) {
      createNumberOfCalls += 1;
      return COMPLETED_FUTURE;
    }

    @Override
    protected CompletableFuture<Void> send(MutationEvent mutation) {
      sendNumberOfCalls += 1;
      return COMPLETED_FUTURE;
    }

    @Override
    public CompletableFuture<Void> close() {
      closeNumberOfCalls += 1;
      return COMPLETED_FUTURE;
    }
  }

  @NonNull
  public static MutationEvent createMutationEvent(Table tableMetadata) {
    return new DefaultMutationEvent(
        tableMetadata,
        OptionalInt.empty(),
        OptionalLong.of(0),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        MutationEventType.UPDATE);
  }
}
