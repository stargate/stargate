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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.common.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Implementation of the {@link HttpAuthenticationMechanism} that authenticates on a header. If a
 * target header exists, request is considered as authenticated.
 *
 * @see HeaderIdentityProvider
 */
public class HeaderBasedAuthenticationMechanism implements HttpAuthenticationMechanism {

  /** The name of the header to be used for the authentication. */
  private final String headerName;

  /** Object mapper for custom response. */
  private final ObjectMapper objectMapper;

  public HeaderBasedAuthenticationMechanism(String headerName, ObjectMapper objectMapper) {
    this.headerName = headerName;
    this.objectMapper = objectMapper;
  }

  /** {@inheritDoc} */
  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    String headerValue = context.request().getHeader(headerName);

    if (null != headerValue) {
      HeaderAuthenticationRequest request =
          new HeaderAuthenticationRequest(headerName, headerValue);
      HttpSecurityUtils.setRoutingContextAttribute(request, context);
      return identityProviderManager.authenticate(request);
    }

    // No suitable header has been found in this request,
    return Uni.createFrom().optional(Optional.empty());
  }

  /** {@inheritDoc} */
  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    return Uni.createFrom()
        .item(new ChallengeData(HttpResponseStatus.UNAUTHORIZED.code(), null, null));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Writes a custom response.
   */
  @Override
  public Uni<Boolean> sendChallenge(RoutingContext context) {
    // keep the original flow, although I don't need the getChallenge
    return getChallenge(context)
        .flatMap(
            challengeData -> {
              int status = challengeData.status;
              context.response().setStatusCode(status);

              // create the response
              String message =
                  "Role unauthorized for operation: Missing token, expecting one in the %s header."
                      .formatted(headerName);
              ApiError apiError = new ApiError(message, status);
              try {
                // try to serialize
                String response = objectMapper.writeValueAsString(apiError);

                // set content type
                context
                    .response()
                    .headers()
                    .set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

                // set content length
                context
                    .response()
                    .headers()
                    .set(HttpHeaders.CONTENT_LENGTH, String.valueOf(response.getBytes().length));

                // write and map to true
                return Uni.createFrom()
                    .completionStage(
                        context.response().write(response).map(true).toCompletionStage());
              } catch (JsonProcessingException e) {
                return Uni.createFrom().item(true);
              }
            });
  }

  /** {@inheritDoc} */
  @Override
  public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
    return Collections.singleton(HeaderAuthenticationRequest.class);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    HttpCredentialTransport transport =
        new HttpCredentialTransport(HttpCredentialTransport.Type.OTHER_HEADER, headerName);
    return Uni.createFrom().item(transport);
  }
}
