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
import java.util.Map;
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

  private DataStore create(
      @Nonnull AuthenticatedUser user,
      @Nonnull DataStoreOptions options,
      @Nullable ClientInfo clientInfo) {
    Persistence.Connection connection;
    if (clientInfo != null) {
      connection = persistence.newConnection(clientInfo);
    } else {
      connection = persistence.newConnection();
    }

    connection.login(user);

    // Note: the user's custom properties (if provided) override any matching properties previously
    // set in DataStoreOptions. This is intentional to give the AuthenticatedUser data more
    // authority.
    Map<String, String> customProperties = user.customProperties();
    if (customProperties != null && !customProperties.isEmpty()) {
      options =
          DataStoreOptions.builder().from(options).putAllCustomProperties(customProperties).build();
    }

    if (options.customProperties() != null) {
      connection.setCustomProperties(options.customProperties());
    }

    return create(connection, options);
  }

  @Override
  public DataStore create(@Nonnull AuthenticatedUser user, @Nonnull DataStoreOptions options) {
    ClientInfo clientInfo = null;
    if (!user.isFromExternalAuth()) {
      // Must have a clientInfo so that an external ClientState is used in order for authorization
      // to be performed
      clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    }
    return create(user, options, clientInfo);
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
