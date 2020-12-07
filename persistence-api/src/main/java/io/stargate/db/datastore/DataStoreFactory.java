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
import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import io.stargate.db.cdc.datastore.CDCEnabledDataStore;
import java.net.InetSocketAddress;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataStoreFactory {
  private final ConfigStore configStore;

  public DataStoreFactory(ConfigStore configStore) {
    this.configStore = configStore;
  }

  /**
   * Creates a new DataStore using the provided connection for querying and with the provided
   * default parameters.
   *
   * @param connection the persistence connection to use for querying.
   * @param options the options for the create data store.
   * @return the created store.
   */
  public DataStore create(Persistence.Connection connection, @Nonnull DataStoreOptions options) {
    Objects.requireNonNull(options);
    PersistenceBackedDataStore persistenceBackedDataStore =
        new PersistenceBackedDataStore(connection, options);
    CDCEnabledDataStore cdcEnabledDataStore =
        new CDCEnabledDataStore(persistenceBackedDataStore, configStore);
    cdcEnabledDataStore.initCDCKeyspaceTable();
    return cdcEnabledDataStore;
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
   * @param options the options for the create data store.
   * @param clientInfo the ClientInfo to be used for creating a connection.
   * @return the created store.
   */
  public DataStore create(
      Persistence persistence,
      @Nullable String userName,
      @Nonnull DataStoreOptions options,
      @Nullable ClientInfo clientInfo) {
    Persistence.Connection connection = persistence.newConnection();
    if (clientInfo != null) {
      connection = persistence.newConnection(clientInfo);
    }

    if (userName != null && !userName.isEmpty()) {
      connection.login(AuthenticatedUser.of(userName));
    }
    return create(connection, options);
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
   * @param options the options for the create data store.
   * @return the created store.
   */
  public DataStore create(
      Persistence persistence, @Nullable String userName, @Nonnull DataStoreOptions options) {
    ClientInfo clientInfo = null;
    if (!Strings.isNullOrEmpty(userName)) {
      // Must have a clientInfo so that an external ClientState is used in order for authorization
      // to be performed
      clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    }
    return create(persistence, userName, options, clientInfo);
  }

  /**
   * Creates a new DataStore on top of the provided persistence. If a username is provided then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed if enabled. ClientInfo
   * clientInfo = null; if (!Strings.isNullOrEmpty(userName)) { // Must have a clientInfo so that an
   * external ClientState is used in order for authorization // to be performed clientInfo = new
   * ClientInfo(new InetSocketAddress("127.0.0.1", 0), null); }
   *
   * <p>return create(persistence, userName, Parameters.defaults(), clientInfo); Creates a new
   * DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(Persistence, DataStoreOptions)} with default options.
   */
  public DataStore create(Persistence persistence) {
    return create(persistence, DataStoreOptions.defaults());
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(Persistence, String, DataStoreOptions)} with a {@code null}
   * userName.
   */
  public DataStore create(Persistence persistence, DataStoreOptions options) {
    return create(persistence, null, options);
  }
}
