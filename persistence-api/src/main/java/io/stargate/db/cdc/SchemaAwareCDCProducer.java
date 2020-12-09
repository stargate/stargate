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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.stargate.db.cdc.api.MutationEvent;
import io.stargate.db.schema.Table;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Represents a schema-aware CDC async producer.
 *
 * <p>Implementors will get the following guarantees:
 *
 * <ul>
 *   <li>For a given table, {@link #createTableSchemaAsync} will be invoked before calling {@link
 *       #publish} with a partition update for that table (awaiting for the creation future to
 *       complete).
 *   <li>In the same way, when table metadata change is first detected, {@link
 *       #createTableSchemaAsync} will be invoked before calling {@link #publish}
 * </ul>
 */
public abstract class SchemaAwareCDCProducer implements CDCProducer {
  private final ConcurrentHashMap<Table, TableSchemaManager> tableSchemaManager =
      new ConcurrentHashMap<>();

  @Override
  public abstract CompletableFuture<Void> init();

  /** Asynchronously publishes the schema of the table. */
  protected abstract CompletableFuture<Void> createTableSchemaAsync(Table table);

  /**
   * Asynchronously publishes the partition update.
   *
   * <p>It will be invoked after {@link SchemaAwareCDCProducer#createTableSchemaAsync} was invoked
   * for a given table and creation future was completed.
   */
  protected abstract CompletableFuture<Void> send(MutationEvent mutation);

  @Override
  public abstract CompletableFuture<Void> close();

  @Override
  public CompletableFuture<Void> publish(MutationEvent mutation) {
    TableSchemaManager schemaManager =
        tableSchemaManager.computeIfAbsent(
            mutation.table(), k -> new TableSchemaManager(this::createTableSchemaAsync));

    return schemaManager
        .ensureCreated(mutation.table())
        // Invoke send() with the version that later affected this table,
        // not the most recent version number.
        .thenCompose(originalSchemaVersion -> send(mutation));
  }

  /** Contains logic to handle different table schema versions. */
  @VisibleForTesting
  static class TableSchemaManager {
    private final Function<Table, CompletableFuture<Void>> createHandler;
    private final ConcurrentHashMap<Table, CompletableFuture<Void>> creators =
        new ConcurrentHashMap<>();

    TableSchemaManager(Function<Table, CompletableFuture<Void>> createHandler) {
      this.createHandler = createHandler;
    }

    /**
     * Ensures that {@link TableSchemaManager#createHandler} is invoked once per different table
     * version.
     *
     * <p>Reuses previous table schema when the version changed but it didn't affect the table uses
     * both hashCode + equality to guard against collisions.
     *
     * @return The UUID of the schema version that caused a changed in the table.
     */
    public CompletableFuture<Void> ensureCreated(Table table) {
      // Try to reuse the table schema for a given table hash code
      return creators.computeIfAbsent(table, k2 -> create(table));
    }

    private CompletableFuture<Void> create(Table table) {
      return this.createHandler.apply(table);
    }
  }
}
