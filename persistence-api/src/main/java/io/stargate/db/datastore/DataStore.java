/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.datastore;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * This will be our interface in to the rest of DSE. By using this rather than calling static
 * methods we have a fighting chance of being able to unit test without starting C*.
 */
public interface DataStore {
  /** The fetch size for SELECT statements */
  int DEFAULT_ROWS_PER_PAGE = 1000;

  /** Create a query using the DSL builder. */
  default QueryBuilder query() {
    return new QueryBuilder(this);
  }

  default CompletableFuture<ResultSet> query(String cql, Object... parameters) {
    return query(cql, Optional.empty(), parameters);
  }

  CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... parameters);

  default PreparedStatement prepare(String cql) {
    return prepare(cql, Optional.empty());
  }

  PreparedStatement prepare(String cql, Optional<Index> index);

  default CompletableFuture<ResultSet> processBatch(
      List<PreparedStatement> statements,
      List<Object[]> vals,
      Optional<ConsistencyLevel> consistencyLevel) {
    throw new UnsupportedOperationException(
        "Batching not supported on " + getClass().getSimpleName());
  }

  /**
   * Returns the current schema.
   *
   * @return The current schema.
   */
  Schema schema();

  /** Wait for schema to agree across the cluster */
  default void waitForSchemaAgreement() {
    for (int count = 0; count < 100; count++) {
      if (isInSchemaAgreement()) {
        return;
      }
      Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
    }
    throw new IllegalStateException("Failed to reach schema agreement after 20 seconds.");
  }

  /** Returns true if in schema agreement */
  boolean isInSchemaAgreement();
}
