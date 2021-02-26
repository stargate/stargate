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

import io.stargate.db.Persistence;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Schema;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps a {@link Persistence} implementation to provide more convenient and high-level access.
 *
 * <p>DataStore provides a number of convenience over {@link Persistence}, like the use of actual
 * java object values (instead of raw byte buffers), automatic handling of paging in its result set,
 * etc.
 */
public interface DataStore extends AsyncQueryExecutor {

  TypedValue.Codec valueCodec();

  /** Create a query using the DSL builder. */
  default QueryBuilder queryBuilder() {
    return new QueryBuilder(schema(), valueCodec(), this);
  }

  <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query);

  /**
   * Returns the current schema.
   *
   * @return The current schema.
   */
  Schema schema();

  /** Returns true if the persistence backend supports secondary indexes */
  boolean supportsSecondaryIndex();

  /** Returns true if the persistence backend supports Storage Attached Indexes. */
  boolean supportsSAI();

  /** Returns true if the persistence backend allows logged batches. */
  boolean supportsLoggedBatches();

  /** Returns true if in schema agreement */
  boolean isInSchemaAgreement();

  /** Wait for schema to agree across the cluster */
  void waitForSchemaAgreement();
}
