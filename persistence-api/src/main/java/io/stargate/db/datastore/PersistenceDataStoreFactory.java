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
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import java.net.InetSocketAddress;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PersistenceDataStoreFactory implements DataStoreFactory {
  private final Persistence persistence;

  public PersistenceDataStoreFactory(Persistence persistence) {
    this.persistence = persistence;
  }

  /**
   * Creates a new DataStore using the provided connection for querying and with the provided
   * default parameters.
   *
   * @param connection the persistence connection to use for querying.
   * @param options the options for the create data store.
   * @return the created store.
   */
  private DataStore create(Persistence.Connection connection, @Nonnull DataStoreOptions options) {
    Objects.requireNonNull(options);
    return new PersistenceBackedDataStore(connection, options);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled.
   *
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *     or the empty string, no login attempt is performed (so no authentication must be setup).
   * @param isFromExternalAuth Whether the request was authenticated using internal Cassandra/DSE
   *     auth mechanisms or used an external source.
   * @param options the options for the create data store.
   * @param clientInfo the ClientInfo to be used for creating a connection.
   * @return the created store.
   */
  private DataStore create(
      @Nullable String userName,
      boolean isFromExternalAuth,
      @Nonnull DataStoreOptions options,
      @Nullable ClientInfo clientInfo) {
    Persistence.Connection connection;
    if (clientInfo != null) {
      connection = persistence.newConnection(clientInfo);
    } else {
      connection = persistence.newConnection();
    }

    if (!isFromExternalAuth) {
      connection.login(AuthenticatedUser.of(userName));
    }

    if (options.customProperties() != null) {
      connection.setCustomProperties(options.customProperties());
    }

    return create(connection, options);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled.
   *
   * @param userName the user name to login for this store. For convenience, if it is {@code null}
   *     or the empty string, no login attempt is performed (so no authentication must be setup).
   * @param options the options for the create data store.
   * @return the created store.
   */
  @Override
  public DataStore create(@Nullable String userName, @Nonnull DataStoreOptions options) {
    return create(userName, false, options);
  }

  @Override
  public DataStore create(
      @Nullable String userName, boolean isFromExternalAuth, @Nonnull DataStoreOptions options) {
    ClientInfo clientInfo = null;
    if (!isFromExternalAuth) {
      // Must have a clientInfo so that an external ClientState is used in order for authorization
      // to be performed
      clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    }
    return create(userName, isFromExternalAuth, options, clientInfo);
  }

  /** @inheritdoc */
  @Override
  public DataStore createInternal() {
    return createInternal(DataStoreOptions.defaults());
  }

  /** @inheritdoc */
  @Override
  public DataStore createInternal(DataStoreOptions options) {
    Persistence.Connection connection = persistence.newConnection();
    return create(connection, options);
  }
}
