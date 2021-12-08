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

import io.stargate.grpc.StargateBearerToken;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.impl.GrpcStubFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * Validates that the auth token is present and pre-builds the gRPC stub before each request.
 *
 * <p>Note that the resource class or method must be annotated with {@link CreateGrpcStub} in order
 * for this filter to run.
 *
 * @see GrpcStubFactory
 */
@Provider
@CreateGrpcStub
public class CreateGrpcStubFilter implements ContainerRequestFilter {

  public static final String GRPC_STUB_KEY = "io.stargate.sgv2.restsvc.resources.GrpcStub";

  private final GrpcClientFactory grpcClientFactory;

  public CreateGrpcStubFilter(GrpcClientFactory grpcClientFactory) {
    this.grpcClientFactory = grpcClientFactory;
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
    }
    context.setProperty(
        GRPC_STUB_KEY,
        grpcClientFactory
            .constructBlockingStub()
            .withCallCredentials(new StargateBearerToken(token)));
  }
}
