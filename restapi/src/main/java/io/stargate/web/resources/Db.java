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
import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.web.docsapi.dao.DocumentDB;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;

public class Db {

  private final DataStore dataStore;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final LoadingCache<String, AuthenticationPrincipal> docsTokensToRoles =
      Caffeine.newBuilder()
          .maximumSize(10_000)
          .expireAfterWrite(Duration.ofMinutes(1))
          .build(this::getAuthenticationPrincipalForToken);
  private final DataStoreFactory dataStoreFactory;

  public Db(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
    this.dataStore =
        dataStoreFactory.createInternal(DataStoreOptions.defaultsWithAutoPreparedQueries());
  }

  public DataStore getDataStore() {
    return this.dataStore;
  }

  public AuthenticationService getAuthenticationService() {
    return authenticationService;
  }

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public AuthenticatedDB getDataStoreForToken(String token) throws UnauthorizedException {
    AuthenticationPrincipal authenticationPrincipal = authenticationService.validateToken(token);
    DataStore dataStore =
        dataStoreFactory.create(
            authenticationPrincipal.getRoleName(),
            authenticationPrincipal.isFromExternalAuth(),
            DataStoreOptions.defaultsWithAutoPreparedQueries());

    return new AuthenticatedDB(dataStore, authenticationPrincipal);
  }

  public AuthenticatedDB getDataStoreForToken(String token, int pageSize, ByteBuffer pagingState)
      throws UnauthorizedException {
    AuthenticationPrincipal authenticationPrincipal = authenticationService.validateToken(token);
    return new AuthenticatedDB(
        getDataStoreInternal(authenticationPrincipal, pageSize, pagingState),
        authenticationPrincipal);
  }

  private DataStore getDataStoreInternal(
      AuthenticationPrincipal authenticationPrincipal, int pageSize, ByteBuffer pagingState) {
    Parameters parameters =
        Parameters.builder()
            .pageSize(pageSize)
            .pagingState(Optional.ofNullable(pagingState))
            .build();

    DataStoreOptions options =
        DataStoreOptions.builder().defaultParameters(parameters).alwaysPrepareQueries(true).build();
    return dataStoreFactory.create(
        authenticationPrincipal.getRoleName(),
        authenticationPrincipal.isFromExternalAuth(),
        options);
  }

  public AuthenticationPrincipal getAuthenticationPrincipalForToken(String token)
      throws UnauthorizedException {
    return authenticationService.validateToken(token);
  }

  public DocumentDB getDocDataStoreForToken(String token) throws UnauthorizedException {
    AuthenticatedDB authenticatedDB = getDataStoreForToken(token);
    return new DocumentDB(
        authenticatedDB.getDataStore(),
        authenticatedDB.getAuthenticationPrincipal(),
        getAuthorizationService());
  }

  public DocumentDB getDocDataStoreForToken(String token, int pageSize, ByteBuffer pageState)
      throws UnauthorizedException {
    if (token == null) {
      throw new UnauthorizedException("Missing token");
    }

    AuthenticationPrincipal authenticationPrincipal;
    try {
      authenticationPrincipal = docsTokensToRoles.get(token);
    } catch (CompletionException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      }
      throw e;
    }

    if (authenticationPrincipal == null) {
      throw new UnauthorizedException("Missing authenticationPrincipal");
    }

    return new DocumentDB(
        getDataStoreInternal(authenticationPrincipal, pageSize, pageState),
        authenticationPrincipal,
        getAuthorizationService());
  }
}
