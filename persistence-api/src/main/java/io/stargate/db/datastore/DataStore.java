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

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Persistence;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Schema;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Wraps a {@link Persistence} implementation to provide more convenient and high-level access.
 *
 * <p>DataStore provides a number of convenience over {@link Persistence}, like the use of actual
 * java object values (instead of raw byte buffers), automatic handling of paging in its result set,
 * etc.
 */
public interface DataStore extends AsyncQueryExecutor {

  /**
   * Creates a new DataStore using the provided connection for querying and with the provided
   * default parameters.
   *
   * @param connection the persistence connection to use for querying.
   * @param options the options for the create data store.
   * @return the created store.
   */
  static DataStore create(Persistence.Connection connection, @Nonnull DataStoreOptions options) {
    Objects.requireNonNull(options);
    return new PersistenceBackedDataStore(connection, options);
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * @param persistence the persistence to use for querying (this method effectively creates a new
   *     {@link Persistence.Connection} underneath).
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *     or the empty string, no login attempt is performed (so no authentication must be setup).
   * @param options the options for the create data store.
   * @return the created store.
   */
  static DataStore create(
      Persistence persistence, @Nullable String userName, @Nonnull DataStoreOptions options) {
    Persistence.Connection connection = persistence.newConnection();
    if (userName != null && !userName.isEmpty()) {
      connection.login(AuthenticatedUser.of(userName));
    }
    return create(connection, options);
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(Persistence, DataStoreOptions)} with default options.
   */
  static DataStore create(Persistence persistence) {
    return create(persistence, DataStoreOptions.defaults());
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(Persistence, String, DataStoreOptions)} with a {@code null}
   * userName.
   */
  static DataStore create(Persistence persistence, DataStoreOptions options) {
    return create(persistence, null, options);
  }

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

  /** Returns true if in schema agreement */
  boolean isInSchemaAgreement();

  /** Wait for schema to agree across the cluster */
  void waitForSchemaAgreement();
}
