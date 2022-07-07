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
import javax.annotation.Nonnull;

public interface DataStoreFactory {

  /**
   * Creates a new DataStore on top of the configured persistence. If the user has been authorized
   * by the persistence itself (i.e. it is not {@link AuthenticatedUser#isFromExternalAuth()} then a
   * {@link ClientInfo} will be passed to {@link
   * Persistence#newConnection(io.stargate.db.ClientInfo)} causing the new connection to have an
   * external ClientState thus causing authorization to be performed at the persistence level (if
   * enabled).
   *
   * @param user the object representing the user to login for this store.
   * @param options the options for the create data store.
   * @return the created store.
   */
  DataStore create(@Nonnull AuthenticatedUser user, @Nonnull DataStoreOptions options);

  /**
   * Creates a new internal DataStore on top of the provided persistence.
   *
   * <p>A shortcut for {@link #createInternal(DataStoreOptions)}.
   */
  DataStore createInternal();

  /**
   * Creates an internal connection with default options intended to be used for system operations.
   *
   * @return the created store.
   */
  DataStore createInternal(DataStoreOptions options);
}
