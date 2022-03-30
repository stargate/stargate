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
package io.stargate.sgv2.common.http;

import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeClientFactory;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

/**
 * Validates that the auth token is present and pre-builds the gRPC client before each request.
 *
 * <p>Note that the resource class or method must be annotated with {@link
 * CreateStargateBridgeClient} in order for this filter to run.
 *
 * @see StargateBridgeClientJerseyFactory
 */
@Provider
@CreateStargateBridgeClient
public abstract class CreateStargateBridgeClientFilter implements ContainerRequestFilter {

  public static final String CLIENT_KEY = StargateBridgeClient.class.getName();
  private static final String HOST_HEADER_NAME = "Host";
  private static final String HOST_HEADER_NAME_LOWER = "host";

  private final StargateBridgeClientFactory stargateBridgeClientFactory;

  public CreateStargateBridgeClientFilter(StargateBridgeClientFactory stargateBridgeClientFactory) {
    this.stargateBridgeClientFactory = stargateBridgeClientFactory;
  }

  /** Wraps the given error message in the format appropriate to the service. */
  protected abstract Response buildError(Status status, String message, MediaType mediaType);

  @Override
  public void filter(ContainerRequestContext context) {
    final String token = getToken(context);
    if (token == null || token.isEmpty()) {
      context.abortWith(
          buildError(Status.UNAUTHORIZED, "Missing or invalid Auth Token", context.getMediaType()));
    } else {
      context.setProperty(
          CLIENT_KEY,
          stargateBridgeClientFactory.newClient(token, getTenantId(context.getHeaders())));
    }
  }

  protected String getToken(ContainerRequestContext context) {
    return context.getHeaderString("X-Cassandra-Token");
  }

  private Optional<String> getTenantId(MultivaluedMap<String, String> headers) {
    String host = headers.getFirst(HOST_HEADER_NAME);
    if (host == null) {
      host = headers.getFirst(HOST_HEADER_NAME_LOWER);
    }
    return extractTenantId(host);
  }

  // Assumes the tenant ID is a UUID prefix of the host name
  private static Optional<String> extractTenantId(String host) {
    if (host == null || host.length() < 36) {
      return Optional.empty();
    }
    try {
      UUID uuid = UUID.fromString(host.substring(0, 36));
      return Optional.of(uuid.toString());
    } catch (IllegalArgumentException __) {
      return Optional.empty();
    }
  }
}
