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
package io.stargate.sgv2.docssvc.resources.schemas;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@ApiImplicitParams({
  @ApiImplicitParam(
      name = "X-Cassandra-Token",
      paramType = "header",
      value = "The token returned from the authorization endpoint. Use this token in each request.",
      required = true)
})
public interface Sgv2KeyspacesResourceApi {
  @Timed
  @GET
  @ApiOperation(
      value = "Get all keyspaces",
      notes = "Retrieve all available keyspaces.",
      response = Sgv2Keyspace.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Keyspace.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response getAllKeyspaces(
      @Context StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @GET
  @ApiOperation(
      value = "Get a keyspace",
      notes = "Return a single keyspace specification.",
      response = Sgv2Keyspace.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Keyspace.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{keyspaceName}")
  Response getOneKeyspace(
      @Context StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @POST
  @ApiOperation(
      value = "Create a keyspace",
      notes = "Create a new keyspace.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 409, message = "Conflict", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response createKeyspace(
      @Context StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      @ApiParam(
              value =
                  "A map representing a keyspace with SimpleStrategy or NetworkTopologyStrategy with default replicas of 1 and 3 respectively \n"
                      + "Simple:\n"
                      + "```json\n"
                      + "{ \"name\": \"killrvideo\", \"replicas\": 1}\n"
                      + "````\n"
                      + "Network Topology:\n"
                      + "```json\n"
                      + "{\n"
                      + "  \"name\": \"killrvideo\",\n"
                      + "   \"datacenters\":\n"
                      + "      [\n"
                      + "         { \"name\": \"dc1\", \"replicas\": 3 },\n"
                      + "         { \"name\": \"dc2\", \"replicas\": 3 },\n"
                      + "      ],\n"
                      + "}\n"
                      + "```")
          JsonNode payload,
      @Context HttpServletRequest request);

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a keyspace", notes = "Delete a single keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{keyspaceName}")
  Response deleteKeyspace(
      @Context StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Context HttpServletRequest request);
}
