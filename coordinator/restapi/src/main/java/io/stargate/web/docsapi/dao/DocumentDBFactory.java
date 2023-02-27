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
package io.stargate.web.docsapi.dao;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Factory class injected into Resources, used to create actual {@link DocumentDB}
 * (using {@link #getDocDBForToken {}; {@link DocumentDB}
 * will abstract some of the access to the underlying Persistence implementation.
 */
public class DocumentDBFactory {
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;

  private final LoadingCache<TokenAndHeaders, DocumentDB> docsTokensToDataStore =
      Caffeine.newBuilder()
          .maximumSize(10_000)
          .expireAfterWrite(Duration.ofMinutes(1))
          .build(this::getDocDBForTokenInternal);

  private final DocsApiConfiguration config;

  private final DataStoreFactory dataStoreFactory;

  public DocumentDBFactory(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      DocsApiConfiguration config) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
    this.config = config;
  }

  public DocumentDB getDocDBForToken(String token, Map<String, String> headers)
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

  private DocumentDB getDocDBForTokenInternal(TokenAndHeaders tokenAndHeaders)
      throws UnauthorizedException {
    AuthenticationSubject authenticationSubject =
        authenticationService.validateToken(tokenAndHeaders.token, tokenAndHeaders.headers);
    return new DocumentDB(
        constructDataStore(authenticationSubject, tokenAndHeaders),
        authenticationSubject,
        authorizationService,
        config);
  }

  private DataStore constructDataStore(
      AuthenticationSubject authenticationSubject, TokenAndHeaders tokenAndHeaders) {
    return dataStoreFactory.create(
        authenticationSubject.asUser(),
        DataStoreOptions.builder()
            .alwaysPrepareQueries(true)
            .putAllCustomProperties(tokenAndHeaders.headers)
            .build());
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
