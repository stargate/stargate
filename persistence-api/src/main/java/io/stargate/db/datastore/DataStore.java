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
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Wraps a {@link Persistence} implementation to provide more convenient and high-level access.
 *
 * <p>DataStore provides a number of convenience over {@link Persistence}, like the use of actual
 * java object values (instead of raw byte buffers), automatic handling of paging in its result
 * set, etc.
 */
public interface DataStore {

  /**
   * Placeholder value that can used to represent an UNSET value in methods of the DataStore.
   *
   * <p>Note that using this value is equivalent to using the underlying persistence unset value
   * ({@link Persistence#unsetValue()}) <em>in the DataStore methods</em>, but is a tad more
   * convenient in practice.
   */
  Object UNSET =
      new Object() {
        @Override
        public String toString() {
          return "<unset>";
        }
      };

  /**
   * Creates a new DataStore using the provided connection for querying and with the provided
   * default parameters.
   *
   * @param connection the persistence connection to use for querying.
   * @param queryParameters the default parameters to use for the queries made on the created
   *                        store. Note that all those parameters can be overridden on a per-query
   *                        basis if needed.
   * @return the created store.
   */
  static DataStore create(Persistence.Connection connection, @Nonnull Parameters queryParameters) {
    Objects.requireNonNull(queryParameters);
    return new PersistenceBackedDataStore(connection, queryParameters);
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * @param persistence the persistence to use for querying (this method effectively creates a new
   *                    {@link Persistence.Connection} underneath).
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *                 or the empty string, no login attempt is performed (so no authentication must
   *                 be setup).
   * @param queryParameters the default parameters to use for the queries made on the created
   *                        store. Note that all those parameters can be overridden on a per-query
   *                        basis if needed.
   * @return the created store.
   */
  static DataStore create(
      Persistence persistence, @Nullable String userName, @Nonnull Parameters queryParameters) {
    Persistence.Connection connection = persistence.newConnection();
    if (userName != null && !userName.isEmpty()) {
      connection.login(AuthenticatedUser.of(userName));
    }
    return create(connection, queryParameters);
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * Same as {@link #create(Persistence, String, Parameters)}, but using
   * {@link Parameters#defaults()} for the default parameters.
   */
  static DataStore create(Persistence persistence, @Nullable String userName) {
    return create(persistence, userName, Parameters.defaults());
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * A shortcut for {@link #create(Persistence, String)} with a {@code null} userName.
   */
  static DataStore create(Persistence persistence) {
    return create(persistence, null);
  }

  /** Create a query using the DSL builder. */
  default QueryBuilder query() {
    return new QueryBuilder(this);
  }

  default CompletableFuture<ResultSet> query(String cql, Object... parameters) {
    return query(cql, Optional.empty(), parameters);
  }

  CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... parameters);

  default CompletableFuture<PreparedStatement> prepare(String cql) {
    return prepare(cql, Optional.empty());
  }

  CompletableFuture<PreparedStatement> prepare(String cql, Optional<Index> index);

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
