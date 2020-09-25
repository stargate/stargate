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
package io.stargate.graphql.graphqlservlet;

import graphql.kickstart.execution.context.GraphQLContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.impl.DefaultClaims;
import java.util.Optional;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import org.dataloader.DataLoaderRegistry;

public class HTTPAwareContextImpl implements GraphQLContext {
  private final DataLoaderRegistry dataLoaderRegistry;
  private Session session;
  private HandshakeRequest handshakeRequest;
  private HttpServletRequest request;
  private HttpServletResponse response;

  private static final String HEADER = "Authorization";
  private static final String PREFIX = "Bearer ";

  public HTTPAwareContextImpl(
      DataLoaderRegistry dataLoaderRegistry,
      HttpServletRequest request,
      HttpServletResponse response) {
    this.dataLoaderRegistry = dataLoaderRegistry;
    this.request = request;
    this.response = response;
  }

  // Web socket
  public HTTPAwareContextImpl(
      DataLoaderRegistry dataLoaderRegistry, Session session, HandshakeRequest handshakeRequest) {
    this.dataLoaderRegistry = dataLoaderRegistry;
    this.session = session;
    this.handshakeRequest = handshakeRequest;
  }

  public String getAuthToken() {
    return request.getHeader("X-Cassandra-Token");
  }

  public String getUserOrRole() {
    if (hasJWTToken()) {
      String authHeader = request.getHeader(HEADER);
      String jwt = authHeader.substring(authHeader.indexOf(PREFIX) + PREFIX.length());
      Claims claims = decodeJWT(jwt);
      if (claims != null) {
        return claims.get("X-Cassandra-User", String.class);
      } else {
        return null;
      }
    }

    return null;
  }

  // Assumes the JWT token has been authenticated
  public static Claims decodeJWT(String jwt) {
    try {
      Claims claims = (DefaultClaims) Jwts.parser().parse(jwt).getBody();
      return claims;
    } catch (Exception e) {
      return null;
    }
  }

  private boolean hasJWTToken() {
    String header = request.getHeader(HEADER);
    return header != null && header.startsWith(PREFIX);
  }

  @Override
  public Optional<Subject> getSubject() {
    return Optional.empty();
  }

  @Override
  public Optional<DataLoaderRegistry> getDataLoaderRegistry() {
    return Optional.ofNullable(dataLoaderRegistry);
  }
}
