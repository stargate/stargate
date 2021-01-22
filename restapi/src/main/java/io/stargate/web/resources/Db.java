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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class Db {

  private final DataStore dataStore;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;

  private final LoadingCache<TokenAndHeaders, DocumentDB> docsTokensToDataStore =
      Caffeine.newBuilder()
          .maximumSize(10_000)
          .expireAfterWrite(Duration.ofMinutes(1))
          .build(this::getDocDataStoreForTokenInternal);
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

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public AuthenticatedDB getDataStoreForToken(String token, Map<String, String> headers)
      throws UnauthorizedException {
    AuthenticationSubject authenticationSubject =
        authenticationService.validateToken(token, headers);
    DataStore dataStore =
        dataStoreFactory.create(
            authenticationSubject.roleName(),
            authenticationSubject.isFromExternalAuth(),
            DataStoreOptions.builder()
                .alwaysPrepareQueries(true)
                .putAllCustomProperties(headers)
                .putAllCustomProperties(authenticationSubject.customProperties())
                .build());

    return new AuthenticatedDB(dataStore, authenticationSubject);
  }

  public AuthenticatedDB getDataStoreForToken(
      String token, int pageSize, ByteBuffer pagingState, Map<String, String> headers)
      throws UnauthorizedException {
    AuthenticationSubject authenticationSubject =
        authenticationService.validateToken(token, headers);
    if (authenticationSubject == null) {
      throw new UnauthorizedException("Missing authenticationSubject");
    }
    return new AuthenticatedDB(
        getDataStoreInternal(authenticationSubject, pageSize, pagingState, headers),
        authenticationSubject);
  }

  private DataStore getDataStoreInternal(
      AuthenticationSubject authenticationSubject,
      int pageSize,
      ByteBuffer pagingState,
      Map<String, String> headers) {
    Parameters parameters =
        Parameters.builder()
            .pageSize(pageSize)
            .pagingState(Optional.ofNullable(pagingState))
            .build();

    DataStoreOptions options =
        DataStoreOptions.builder()
            .defaultParameters(parameters)
            .alwaysPrepareQueries(true)
            .putAllCustomProperties(headers)
            .putAllCustomProperties(authenticationSubject.customProperties())
            .build();
    return dataStoreFactory.create(
        authenticationSubject.roleName(), authenticationSubject.isFromExternalAuth(), options);
  }

  private DocumentDB getDocDataStoreForTokenInternal(TokenAndHeaders tokenAndHeaders)
      throws UnauthorizedException {
    AuthenticatedDB authenticatedDB =
        getDataStoreForToken(tokenAndHeaders.token, tokenAndHeaders.headers);
    return new DocumentDB(
        authenticatedDB.getDataStore(),
        authenticatedDB.getAuthenticationSubject(),
        getAuthorizationService());
  }

  public AuthenticationSubject getAuthenticationSubjectForToken(TokenAndHeaders tokenAndHeaders)
      throws UnauthorizedException {
    return authenticationService.validateToken(tokenAndHeaders.token, tokenAndHeaders.headers);
  }

  public DocumentDB getDocDataStoreForToken(String token, Map<String, String> headers)
      throws UnauthorizedException {
    if (token == null) {
      throw new UnauthorizedException("Missing token");
    }

    try {
      return docsTokensToDataStore.get(TokenAndHeaders.create(token, headers));
    } catch (CompletionException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      }
      throw e;
    }
  }

  static class TokenAndHeaders {
    private static final String HOST_HEADER = "Host";
    private final String token;
    private final Map<String, String> headers;

    static TokenAndHeaders create(String token, Map<String, String> headers) {
      return new TokenAndHeaders(token, filterHeaders(headers));
    }

    private static Map<String, String> filterHeaders(Map<String, String> headers) {
      return headers.entrySet().stream()
          .filter(e -> e.getKey().equalsIgnoreCase(HOST_HEADER))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private TokenAndHeaders(String token, Map<String, String> headers) {
      this.token = token;
      this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TokenAndHeaders that = (TokenAndHeaders) o;
      return Objects.equals(token, that.token) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(token, headers);
    }
  }
}
