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
package io.stargate.sgv2.restsvc.resources;

import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeClientFactory;
import io.stargate.sgv2.restsvc.impl.GrpcClientJerseyFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * Validates that the auth token is present and pre-builds the gRPC client before each request.
 *
 * <p>Note that the resource class or method must be annotated with {@link CreateGrpcStub} in order
 * for this filter to run.
 *
 * @see GrpcClientJerseyFactory
 */
@Provider
@CreateGrpcStub
public class CreateGrpcClientFilter implements ContainerRequestFilter {

  public static final String GRPC_CLIENT_KEY = StargateBridgeClient.class.getName();
  private static final String HOST_HEADER_NAME = "Host";
  private static final String HOST_HEADER_NAME_LOWER = "host";

  private final StargateBridgeClientFactory stargateBridgeClientFactory;

  public CreateGrpcClientFilter(StargateBridgeClientFactory stargateBridgeClientFactory) {
    this.stargateBridgeClientFactory = stargateBridgeClientFactory;
  }

  @Override
  public void filter(ContainerRequestContext context) {
    String token = context.getHeaderString("X-Cassandra-Token");
    if (token == null || token.isEmpty()) {
      context.abortWith(
          Response.status(Response.Status.UNAUTHORIZED)
              .entity(
                  new RestServiceError(
                      "Missing or invalid Auth Token",
                      Response.Status.UNAUTHORIZED.getStatusCode()))
              .build());
    } else {
      context.setProperty(
          GRPC_CLIENT_KEY,
          stargateBridgeClientFactory.newClient(token, getTenantId(context.getHeaders())));
    }
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
