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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.schema.TableMetadata;

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
public abstract class AsyncCDCProducer implements CDCProducer {
  private final ConcurrentHashMap<UUID, TableSchemaManager> tableSchemaManager =
      new ConcurrentHashMap<>();

  @Override
  public abstract CompletableFuture<Void> init(Map<String, Object> options);

  /** Asynchronously publishes the schema of the table. */
  protected abstract CompletableFuture<Void> createTableSchemaAsync(TableMetadata table);

  /**
   * Asynchronously publishes the partition update.
   *
   * <p>It will be invoked after {@link AsyncCDCProducer#createTableSchemaAsync} was invoked for a
   * given table and creation future was completed.
   */
  protected abstract CompletableFuture<Void> send(MutationEvent mutation);

  public abstract void close() throws Exception;

  @Override
  public CompletableFuture<Void> publish(MutationEvent mutation) {
    TableSchemaManager schemaManager =
        tableSchemaManager.computeIfAbsent(
            mutation.getTable().getId(), k -> new TableSchemaManager(this::createTableSchemaAsync));

    return schemaManager
        .ensureCreated(mutation.getTable())
        // Invoke send() with the version that later affected this table,
        // not the most recent version number.
        .thenCompose(originalSchemaVersion -> send(mutation));
  }

  /** Contains logic to handle different table schema versions. */
  @VisibleForTesting
  static class TableSchemaManager {
    private final Function<TableMetadata, CompletableFuture<Void>> createHandler;

    TableSchemaManager(Function<TableMetadata, CompletableFuture<Void>> createHandler) {
      this.createHandler = createHandler;
    }

    private final ConcurrentHashMap<TableIdentityKey, CompletableFuture<Void>> creators =
        new ConcurrentHashMap<>();

    /**
     * Ensures that {@link TableSchemaManager#createHandler} is invoked once per different table
     * version.
     *
     * <p>Reuses previous table schema when the version changed but it didn't affect the table uses
     * both hashCode + equality to guard against collisions.
     *
     * @return The UUID of the schema version that caused a changed in the table.
     */
    CompletableFuture<Void> ensureCreated(TableMetadata table) {
      TableIdentityKey identity = new TableIdentityKey(table);

      // Try to reuse the table schema for a given table hash code
      return creators.computeIfAbsent(identity, k2 -> create(table));
    }

    private CompletableFuture<Void> create(TableMetadata table) {
      return this.createHandler.apply(table);
    }

    /** A hash map key that uses identity checks, valid for immutable objects. */
    private static class TableIdentityKey {
      private final Object identity;

      TableIdentityKey(TableMetadata table) {
        identity = table.getIdentity();
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableIdentityKey that = (TableIdentityKey) o;
        // Use reference equality
        return identity == that.identity;
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(identity);
      }
    }
  }
}
