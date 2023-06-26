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
package io.stargate.db;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;

/**
 * A persistence layer that can be queried.
 *
 * <p>This is the interface that stargate API extensions uses (either directly, or through the
 * higher level {@link DataStore} API which wraps an instance of this interface) to query the
 * underlying store, and thus the one interface that persistence extensions must implement.
 */
public interface Persistence extends SchemaAgreementChecker {

  boolean SUPPORTS_SECONDARY_INDEX =
      Boolean.parseBoolean(System.getProperty("stargate.persistence.2i.support.default", "true"));

  /** Name describing the persistence implementation. */
  String name();

  /** The current schema. */
  Schema schema();

  /**
   * Registers a listener whose methods will be called when either the schema or the topology of the
   * persistence changes.
   */
  void registerEventListener(EventListener listener);

  Authenticator getAuthenticator();

  /**
   * Notify the other stargate nodes whether RPC is or is not ready on this node.
   *
   * @param status true means that RPC is ready, otherwise RPC is not ready.
   */
  void setRpcReady(boolean status);

  /**
   * Creates a new connection for an "external" client identified by the provided info.
   *
   * @param clientInfo info identifying the client for which the connection is created.
   * @return the newly created persistence connection.
   */
  Connection newConnection(ClientInfo clientInfo);

  /**
   * Creates a new "internal" connection, that is one with no client information.
   *
   * @return the newly created persistence connection.
   */
  Connection newConnection();

  /**
   * The object that should be used to act as an 'unset' value for this persistence (for use in
   * {@link Statement#values()}).
   *
   * <p>Please note that persistence implementations are allowed to use <b>reference equality</b> to
   * detect this value, so the object returned by this method should be used "as-is" and should
   * <b>not</b> be copied (through {@link ByteBuffer#duplicate()} or any other method).
   */
  ByteBuffer unsetValue();

  /**
   * Returns <code>true</code> if the local schema agrees with all storage nodes, <code>false</code>
   * otherwise.
   */
  boolean isInSchemaAgreementWithStorage();

  default boolean supportsSecondaryIndex() {
    return SUPPORTS_SECONDARY_INDEX;
  }

  /** Returns true if the persistence backend supports Storage Attached Indexes. */
  boolean supportsSAI();

  default boolean supportsLoggedBatches() {
    return true;
  }

  /**
   * Returns <code>false</code>> if the persistence implementation cannot reasonably expect to reach
   * schema agreement with storage nodes assuming no external intervention.
   */
  boolean isSchemaAgreementAchievable();

  /**
   * Persistence-specific CQL options to be returned in SUPPORTED response messages.
   *
   * <p><b>Important:</b> At a minimum, this must return the {@code "CQL_VERSION"} option.
   */
  Map<String, List<String>> cqlSupportedOptions();

  /**
   * Execute AUTH_RESPONSE request handling asynchronously on the correct thread pool.
   *
   * @param handler a runnable that handles a AUTH_RESPONSE request
   */
  void executeAuthResponse(Runnable handler);

  /**
   * Allows the persistence backend to modify the keyspace name for a given set of connection
   * properties. This can be used to provide a mapping, for example, from an external keyspace name
   * and an internal one.
   */
  default String decorateKeyspaceName(
      String keyspaceName, Map<String, String> connectionProperties) {
    return keyspaceName;
  }

  /**
   * A connection to the persistence.
   *
   * <p>It is through this object that a user can be logged in and that the persistence can be
   * queried.
   */
  interface Connection extends SchemaAgreementChecker {

    /** The underlying persistence on which this is a connection. */
    Persistence persistence();

    /**
     * Login a user on that connection.
     *
     * @param user the user to login.
     * @throws AuthenticationException if the user is not authorized to login.
     */
    void login(AuthenticatedUser user) throws AuthenticationException;

    /** The user logged in, if any. */
    Optional<AuthenticatedUser> loggedUser();

    /** Information on the client for which the connection was created, if any. */
    Optional<ClientInfo> clientInfo();

    /**
     * The default keyspace that is in use on this connection, if any (that is, if a {@code USE}
     * query has been performed on this connection).
     */
    Optional<String> usedKeyspace();

    /**
     * Gets a cached prepared statement if it's available.
     *
     * @param query the query to prepare.
     * @param parameters the parameters for the preparation (note that preparation only use a subset
     *     of parameters).
     * @return a cached prepared statement, null if it's not cached.
     */
    default Result.Prepared getPrepared(String query, Parameters parameters) {
      return null;
    }

    /**
     * Prepare a query on this connection.
     *
     * @param query the query to prepare.
     * @param parameters the parameters for the preparation (note that preparation only use a subset
     *     of parameters).
     * @return a future that, on completion of the preparation, provides its result. Please see the
     *     {@link #execute} method for warnings regarding the completion of that future. The same
     *     warnings apply for this method as well.
     */
    CompletableFuture<Result.Prepared> prepare(String query, Parameters parameters);

    /**
     * Executes a statement on this connection.
     *
     * @param statement the statement to execute.
     * @param parameters the parameters for the execution.
     * @param queryStartNanoTime the start time of the query, as a nano time returned by {@link
     *     System#nanoTime()}.
     * @return a future that, on completion of the execution, provides its result. Please note that
     *     persistence implementations are allowed to complete this future on an internal, and
     *     potentially performance sensitive, thread. As such, consumers should <b>not</b> "chain"
     *     costly/blocking operations on this future without passing their own executor. In other
     *     words, do not do something like: <code>
     *       execute(...).thenRun(() -> { someBlockingOperation(); })
     *     </code> Instead, use one of the "async" variants of {@link CompletableFuture}, namely:
     *     <code>
     *       // you can also provide your own executor below obviously
     *       execute(...).thenRunAsync(() -> { someBlockingOperation(); })
     *     </code>
     */
    CompletableFuture<Result> execute(
        Statement statement, Parameters parameters, long queryStartNanoTime);

    /**
     * Executes a batch on this connection.
     *
     * @param batch the batch to execute.
     * @param parameters the parameters for the batch execution.
     * @return a future that, on completion of the execution, provides its result. Please see the
     *     {@link #execute} method for warnings regarding the completion of that future. The same
     *     warnings apply for this method as well.
     */
    CompletableFuture<Result> batch(Batch batch, Parameters parameters, long queryStartNanoTime);

    /** Adds properties to be used by persistence backend (if supported) */
    default void setCustomProperties(Map<String, String> customProperties) {}

    /**
     * Converts a custom paging position to a persistence-specific paging state.
     *
     * @param position identifies desired paging position
     * @param parameters request execution parameters (these should generally be the same or
     *     compatible to what will be used for executing the query with the returned paging state).
     * @see Parameters#pagingState()
     */
    ByteBuffer makePagingState(PagingPosition position, Parameters parameters);

    /** Makes a new {@link RowDecorator} for the given table. */
    RowDecorator makeRowDecorator(TableName table);

    /**
     * Checks whether this coordinator in schema agreement with the other nodes in the cluster. This
     * is the same as {@code Persistence#isInSchemaAgreement()}, except that it *may* consider other
     * connection specific properties to determine whether schema is in agreement.
     *
     * @return true if schema is agreement; otherwise false.
     */
    default boolean isInSchemaAgreement() {
      return persistence().isInSchemaAgreement();
    }
  }
}
