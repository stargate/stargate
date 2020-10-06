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
package io.stargate.web.resources;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.ClientState;
import io.stargate.db.DefaultQueryOptions;
import io.stargate.db.Persistence;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import java.nio.ByteBuffer;
import java.util.Collection;
import javax.ws.rs.NotFoundException;

public class Db {
  private final Persistence persistence;
  private final DataStore dataStore;
  private final AuthenticationService authenticationService;

  public Collection<Table> getTables(DataStore dataStore, String keyspaceName) {
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    return keyspace.tables();
  }

  public Table getTable(DataStore dataStore, String keyspaceName, String table) {
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    Table tableMetadata = keyspace.table(table);
    if (tableMetadata == null) {
      throw new NotFoundException(String.format("table '%s' not found", table));
    }
    return tableMetadata;
  }

  public Db(final Persistence<?, ?, ?> persistence, AuthenticationService authenticationService) {
    this.authenticationService = authenticationService;
    this.persistence = persistence;
    ClientState clientState = persistence.newClientState("");
    QueryState queryState = persistence.newQueryState(clientState);
    this.dataStore = persistence.newDataStore(queryState, null);
  }

  public DataStore getDataStore() {
    return this.dataStore;
  }

  public Persistence<?, ?, ?> getPersistence() {
    return this.persistence;
  }

  public DataStore getDataStoreForToken(String token) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = this.persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = this.persistence.newQueryState(clientState);

    return this.persistence.newDataStore(queryState, null);
  }

  public DataStore getDataStoreForToken(String token, int pageSize, ByteBuffer pagingState)
      throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = this.persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = this.persistence.newQueryState(clientState);

    QueryOptions queryOptions =
        DefaultQueryOptions.builder()
            .options(
                DefaultQueryOptions.SpecificOptions.builder()
                    .pageSize(pageSize)
                    .pagingState(pagingState)
                    .build())
            .build();

    return this.persistence.newDataStore(queryState, queryOptions);
  }

  public DocumentDB getDocDataStoreForToken(String token) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = persistence.newQueryState(clientState);

    return new DocumentDB(persistence.newDataStore(queryState, null));
  }

  public DocumentDB getDocDataStoreForToken(String token, int pageSize, ByteBuffer pageState)
      throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = persistence.newQueryState(clientState);
    QueryOptions queryOptions =
        DefaultQueryOptions.builder()
            .options(
                DefaultQueryOptions.SpecificOptions.builder()
                    .pageSize(pageSize)
                    .pagingState(pageState)
                    .build())
            .build();

    return new DocumentDB(persistence.newDataStore(queryState, queryOptions));
  }
}
