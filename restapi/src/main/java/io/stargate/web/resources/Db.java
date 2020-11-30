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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.NotFoundException;

public class Db {

  private final Persistence persistence;
  private final DataStore dataStore;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final LoadingCache<String, String> docsTokensToRoles =
      Caffeine.newBuilder()
          .maximumSize(10_000)
          .expireAfterWrite(1, TimeUnit.MINUTES)
          .build(token -> getRoleNameForToken(token));

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

  public Db(
      final Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.persistence = persistence;
    this.dataStore = DataStore.create(persistence);
  }

  public DataStore getDataStore() {
    return this.dataStore;
  }

  public Persistence getPersistence() {
    return this.persistence;
  }

  public AuthenticationService getAuthenticationService() {
    return authenticationService;
  }

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public DataStore getDataStoreForToken(String token) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    return DataStore.create(persistence, storedCredentials.getRoleName());
  }

  public DataStore getDataStoreForToken(String token, int pageSize, ByteBuffer pagingState)
      throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    Parameters parameters =
        ImmutableParameters.builder()
            .pageSize(pageSize)
            .pagingState(Optional.ofNullable(pagingState))
            .build();

    return DataStore.create(this.persistence, storedCredentials.getRoleName(), parameters);
  }

  public String getRoleNameForToken(String token) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    return storedCredentials.getRoleName();
  }

  public DocumentDB getDocDataStoreForToken(String token) throws UnauthorizedException {
    if (token == null) {
      throw new UnauthorizedException("Missing token");
    }
    String role;
    try {
      role = docsTokensToRoles.get(token);
    } catch (CompletionException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      }
      throw e;
    }
    return new DocumentDB(DataStore.create(persistence, role));
  }

  public DocumentDB getDocDataStoreForToken(String token, int pageSize, ByteBuffer pageState)
      throws UnauthorizedException {
    if (token == null) {
      throw new UnauthorizedException("Missing token");
    }
    Parameters parameters =
        Parameters.builder().pageSize(pageSize).pagingState(Optional.ofNullable(pageState)).build();
    String role;
    try {
      role = docsTokensToRoles.get(token);
    } catch (CompletionException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      }
      throw e;
    }
    return new DocumentDB(DataStore.create(persistence, role, parameters));
  }

  public boolean isDse() {
    return persistence.name().equals("DataStax Enterprise");
  }
}
