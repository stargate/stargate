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

package io.stargate.sgv2.docsapi.api.v2.example;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.v2.example.model.dto.KeyspaceExistsResponse;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Example resource showcasing the OpenAPI v3 annotations and the reactive implementation. */
@Path("/v2/example")
@Produces(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = Constants.OPEN_API_DEFAULT_SECURITY_SCHEME)
public class ExampleResource {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleResource.class);

  @Inject StargateRequestInfo requestInfo;

  @Inject DataStoreProperties dataStoreProperties;

  @Inject DocumentProperties documentProperties;

  @Operation(summary = "Keyspace exists", description = "Checks if the given keyspace exists.")
  @Parameters({
    @Parameter(
        name = "name",
        description = "Name of the keyspace.",
        in = ParameterIn.PATH,
        example = "cycling")
  })
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Call successful.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @org.eclipse.microprofile.openapi.annotations.media.Schema(
                      implementation = KeyspaceExistsResponse.class))
        }),
    @APIResponse(
        responseCode = "401",
        description = "Unauthorized.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @org.eclipse.microprofile.openapi.annotations.media.Schema(
                      implementation = ApiError.class))
        })
  })
  @GET
  @Path("keyspace-exists/{name}")
  public Uni<RestResponse<KeyspaceExistsResponse>> keyspaceExists(@PathParam("name") String name) {
    LOG.info(
        "Data store supports secondary indexes {}.", dataStoreProperties.secondaryIndexesEnabled());
    LOG.info("Document max depth {}.", documentProperties.maxDepth());

    Schema.DescribeKeyspaceQuery describeKeyspaceQuery =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(name).build();

    return requestInfo
        .getStargateBridge()
        .describeKeyspace(describeKeyspaceQuery)
        .map(r -> new KeyspaceExistsResponse(name, r.hasCqlKeyspace()))
        .onFailure()
        .recoverWithUni(
            t -> {
              if (t instanceof StatusRuntimeException sre) {
                Status status = sre.getStatus();
                if (Status.Code.NOT_FOUND.equals(status.getCode())) {
                  return Uni.createFrom().item(new KeyspaceExistsResponse(name, false));
                }
              }
              return Uni.createFrom().failure(t);
            })
        .map(RestResponse::ok);
  }
}
