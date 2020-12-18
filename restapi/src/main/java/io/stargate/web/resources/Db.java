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
import io.stargate.auth.AuthenticationSubject;
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
  private final LoadingCache<String, AuthenticationSubject> docsTokensToRoles =
      Caffeine.newBuilder()
          .maximumSize(10_000)
          .expireAfterWrite(Duration.ofMinutes(1))
          .build(this::getAuthenticationSubjectForToken);
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
    AuthenticationSubject authenticationSubject = authenticationService.validateToken(token);
    DataStore dataStore =
        dataStoreFactory.create(
            authenticationSubject.roleName(),
            authenticationSubject.isFromExternalAuth(),
            DataStoreOptions.defaultsWithAutoPreparedQueries());

    return new AuthenticatedDB(dataStore, authenticationSubject);
  }

<<<<<<< HEAD
  public AuthenticatedDB getDataStoreForToken(String token, int pageSize, ByteBuffer pagingState)
      throws UnauthorizedException {
    AuthenticationSubject authenticationSubject = authenticationService.validateToken(token);
    return new AuthenticatedDB(
        getDataStoreInternal(authenticationSubject, pageSize, pagingState), authenticationSubject);
=======
  private DataStore getDataStoreInternal(String role) {
    DataStoreOptions options = DataStoreOptions.builder().alwaysPrepareQueries(true).build();
    return dataStoreFactory.create(role, options);
>>>>>>> Remove validation of columns, we'll see if that's faster
  }

  private DataStore getDataStoreInternal(
      AuthenticationSubject authenticationSubject, int pageSize, ByteBuffer pagingState) {
    Parameters parameters =
        Parameters.builder()
            .pageSize(pageSize)
            .pagingState(Optional.ofNullable(pagingState))
            .build();

    DataStoreOptions options =
        DataStoreOptions.builder().defaultParameters(parameters).alwaysPrepareQueries(true).build();
    return dataStoreFactory.create(
        authenticationSubject.roleName(), authenticationSubject.isFromExternalAuth(), options);
  }

  public AuthenticationSubject getAuthenticationSubjectForToken(String token)
      throws UnauthorizedException {
    return authenticationService.validateToken(token);
  }

  public DocumentDB getDocDataStoreForToken(String token) throws UnauthorizedException {
    AuthenticatedDB authenticatedDB = getDataStoreForToken(token);
    return new DocumentDB(
        authenticatedDB.getDataStore(),
        authenticatedDB.getAuthenticationSubject(),
        getAuthorizationService());
  }

  public DocumentDB getDocDataStoreForToken(String token, int pageSize, ByteBuffer pageState)
      throws UnauthorizedException {
    if (token == null) {
      throw new UnauthorizedException("Missing token");
    }

    AuthenticationSubject authenticationSubject;
    try {
      authenticationSubject = docsTokensToRoles.get(token);
    } catch (CompletionException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      }
      throw e;
    }

    if (authenticationSubject == null) {
      throw new UnauthorizedException("Missing authenticationSubject");
    }

    return new DocumentDB(
        getDataStoreInternal(authenticationSubject, pageSize, pageState),
        authenticationSubject,
        getAuthorizationService());
  }
}
