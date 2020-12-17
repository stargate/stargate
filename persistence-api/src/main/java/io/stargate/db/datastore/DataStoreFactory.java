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

import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DataStoreFactory {

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
  DataStore create(@Nullable String userName, @Nonnull DataStoreOptions options);

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
   * <p>A shortcut for {@link #create(DataStoreOptions)} with default options.
   */
  default DataStore create() {
    return create(DataStoreOptions.defaults());
  }

  /**
   * Creates a new DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #create(String, DataStoreOptions)} with a {@code null} userName.
   */
  default DataStore create(DataStoreOptions options) {
    return create(null, options);
  }
}
