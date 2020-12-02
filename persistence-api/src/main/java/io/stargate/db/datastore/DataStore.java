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

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.BatchType;
import io.stargate.db.ClientInfo;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.schema.Schema;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Wraps a {@link Persistence} implementation to provide more convenient and high-level access.
 *
 * <p>DataStore provides a number of convenience over {@link Persistence}, like the use of actual
 * java object values (instead of raw byte buffers), automatic handling of paging in its result set,
 * etc.
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
   * @param queryParameters the default parameters to use for the queries made on the created store.
   *     Note that all those parameters can be overridden on a per-query basis if needed.
   * @return the created store.
   */
  static DataStore create(Persistence.Connection connection, @Nonnull Parameters queryParameters) {
    Objects.requireNonNull(queryParameters);
    return new PersistenceBackedDataStore(connection, queryParameters);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled.
   *
   * @param persistence the persistence to use for querying (this method effectively creates a new
   *     {@link Persistence.Connection} underneath).
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *     or the empty string, no login attempt is performed (so no authentication must be setup).
   * @param queryParameters the default parameters to use for the queries made on the created store.
   *     Note that all those parameters can be overridden on a per-query basis if needed.
   * @param clientInfo the ClientInfo to be used for creating a connection.
   * @return the created store.
   */
  static DataStore create(
      Persistence persistence,
      @Nullable String userName,
      @Nonnull Parameters queryParameters,
      @Nullable ClientInfo clientInfo) {
    Persistence.Connection connection = persistence.newConnection();
    if (clientInfo != null) {
      connection = persistence.newConnection(clientInfo);
    }

    if (userName != null && !userName.isEmpty()) {
      connection.login(AuthenticatedUser.of(userName));
    }
    return create(connection, queryParameters);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled.
   *
   * @param persistence the persistence to use for querying (this method effectively creates a new
   *     {@link Persistence.Connection} underneath).
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *     or the empty string, no login attempt is performed (so no authentication must be setup).
   * @param queryParameters the default parameters to use for the queries made on the created store.
   *     Note that all those parameters can be overridden on a per-query basis if needed.
   * @return the created store.
   */
  static DataStore create(
      Persistence persistence, @Nullable String userName, @Nonnull Parameters queryParameters) {
    ClientInfo clientInfo = null;
    if (!Strings.isNullOrEmpty(userName)) {
      // Must have a clientInfo so that an external ClientState is used in order for authorization
      // to be performed
      clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    }
    return create(persistence, userName, queryParameters, clientInfo);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled.
   *
   * <p>Same as {@link #create(Persistence, String, Parameters)}, but using {@link
   * Parameters#defaults()} for the default parameters.
   */
  static DataStore create(Persistence persistence, @Nullable String userName) {
    ClientInfo clientInfo = null;
    if (!Strings.isNullOrEmpty(userName)) {
      // Must have a clientInfo so that an external ClientState is used in order for authorization
      // to be performed
      clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    }

    return create(persistence, userName, Parameters.defaults(), clientInfo);
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(Persistence, String)} with a {@code null} userName.
   */
  static DataStore create(Persistence persistence) {
    return create(persistence, null);
  }

  /** Create a query using the DSL builder. */
  default QueryBuilder query() {
    return new QueryBuilder(this);
  }

  /**
   * Executes the provided query against this data store.
   *
   * <p>This is a shortcut for {@link #query(String, UnaryOperator, Object...)} but where the
   * execution {@link Parameters} are the default ones of the data store.
   */
  default CompletableFuture<ResultSet> query(String queryString, Object... values) {
    return query(queryString, p -> p, values);
  }

  /**
   * Executes the provided query against this data store.
   *
   * <p>This is a shortcut for {@link #query(String, UnaryOperator, Object...)} where the data store
   * default parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> query(
      String queryString, ConsistencyLevel consistencyLevel, Object... values) {
    return query(queryString, p -> p.withConsistencyLevel(consistencyLevel), values);
  }

  /**
   * Executes the provided query against this data store.
   *
   * @param queryString the query to execute.
   * @param parametersModifier a function called on the default parameters of this data store (the
   *     instance provided when building the data store) and whose result parameters are used for
   *     the query execution.
   * @param values the (positional) values for the bind variables in {@code queryString}, if any.
   * @return a future with a {@link ResultSet} object to access the result of the query. The future
   *     is complete as soon as some initial result for the query is available, which for paging
   *     queries means only the first page of the result set. As for {@link
   *     Persistence.Connection#execute}, this future can be completed on a sensitive thread and one
   *     should not chain blocking operations on this future <b>including</b> iterating over the
   *     whole result set (as this may be block when paging kicks in to query further pages). In
   *     other words, <b>do not</b> do:
   *     <pre>
   *   query(...).thenAccept(rs -> { for (Row r : rs) {...} });
   * </pre>
   *     Use {@link CompletableFuture#thenAcceptAsync} instead in that case.
   */
  CompletableFuture<ResultSet> query(
      String queryString, UnaryOperator<Parameters> parametersModifier, Object... values);

  /** Prepares the provided query against this data store. */
  CompletableFuture<PreparedStatement> prepare(String queryString);

  /**
   * Executes the provided bound statements as a batch against this data store.
   *
   * <p>This is a shortcut for {@link #batch(List, UnaryOperator)} where the data store default
   * parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> batch(
      List<PreparedStatement.Bound> statements, ConsistencyLevel consistencyLevel) {
    return batch(statements, p -> p.withConsistencyLevel(consistencyLevel));
  }

  /**
   * Executes the provided bound statements as a batch against this data store.
   *
   * <p>This is a shortcut for {@link #batch(List, BatchType, UnaryOperator)} where batch type
   * defaults to "logged".
   */
  default CompletableFuture<ResultSet> batch(
      List<PreparedStatement.Bound> statements, UnaryOperator<Parameters> parametersModifier) {
    return batch(statements, BatchType.LOGGED, parametersModifier);
  }

  /**
   * Executes the provided bound statements as a batch against this data store.
   *
   * @param statements the statements to execute as a batch.
   * @param batchType the type of the batch.
   * @param parametersModifier a function called on the default parameters of this data store (the
   *     instance provided when building the data store) and whose result parameters are used for
   *     the query execution.
   * @return a future with a {@link ResultSet} object to access the result of the query. See {@link
   *     #query(String, UnaryOperator, Object...)} javadoc for details on that result set; the same
   *     description applies here.
   */
  CompletableFuture<ResultSet> batch(
      List<PreparedStatement.Bound> statements,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier);

  /**
   * Executes the provided queries as a logged batch against this data store.
   *
   * @param queries the queries to execute as a batch.
   * @return a future with a {@link ResultSet} object to access the result of the query.
   */
  CompletableFuture<ResultSet> batch(List<String> queries);

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
