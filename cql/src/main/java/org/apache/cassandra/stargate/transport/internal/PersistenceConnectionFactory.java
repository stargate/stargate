/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.transport.internal;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Persistence;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;

class PersistenceConnectionFactory {

  static final String TOKEN_PAYLOAD_KEY = "stargate.bridge.token";
  static final String TENANT_PAYLOAD_KEY = "stargate.bridge.tenantId";

  private static final String TENANT_ID_HEADER_NAME = "tenant_id";

  private static final int CACHE_TTL_SECS =
      Integer.getInteger("stargate.cql.connection_cache_ttl_seconds", 60);
  private static final int CACHE_MAX_SIZE =
      Integer.getInteger("stargate.cql.connection_cache_max_size", 10_000);

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final LoadingCache<CacheKey, Persistence.Connection> connectionCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECS))
          .maximumSize(CACHE_MAX_SIZE)
          .build(this::buildConnection);

  PersistenceConnectionFactory(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  public Persistence.Connection newConnection(Map<String, ByteBuffer> customPayload) {
    ByteBuffer token = customPayload == null ? null : customPayload.get(TOKEN_PAYLOAD_KEY);
    ByteBuffer tenantId = customPayload == null ? null : customPayload.get(TENANT_PAYLOAD_KEY);
    try {
      return connectionCache.get(new CacheKey(token, tenantId));
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UnauthorizedException) {
        throw new AuthenticationException(cause.getMessage());
      } else {
        throw e;
      }
    }
  }

  private Persistence.Connection buildConnection(CacheKey key) throws UnauthorizedException {

    String token = key.decodeToken();
    String tenantId = key.decodeTenantId();

    Map<String, String> headers =
        tenantId == null
            ? Collections.emptyMap()
            : Collections.singletonMap(TENANT_ID_HEADER_NAME, tenantId);
    AuthenticatedUser user =
        token == null ? null : authenticationService.validateToken(token, headers).asUser();

    Persistence.Connection connection = persistence.newConnection();
    if (user != null) {
      connection.login(user);
      connection.clientInfo().ifPresent(c -> c.setAuthenticatedUser(user));
    }
    connection.setCustomProperties(headers);
    return connection;
  }

  static class CacheKey {
    final ByteBuffer token;
    final ByteBuffer tenantId;

    CacheKey(ByteBuffer token, ByteBuffer tenantId) {
      this.token = token;
      this.tenantId = tenantId;
    }

    String decodeToken() {
      return token == null ? null : StandardCharsets.UTF_8.decode(token).toString();
    }

    String decodeTenantId() {
      return tenantId == null ? null : StandardCharsets.UTF_8.decode(tenantId).toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof CacheKey) {
        CacheKey that = (CacheKey) other;
        return Objects.equals(this.token, that.token)
            && Objects.equals(this.tenantId, that.tenantId);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(token, tenantId);
    }
  }
}
