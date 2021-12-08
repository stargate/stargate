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
package io.stargate.sgv2.restsvc.resources.schemas;

import com.codahale.metrics.annotation.Timed;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2IndexAddRequest;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;
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
public interface Sgv2IndexesResourceApi {

  @Timed
  @GET
  @ApiOperation(
      value = "Get all indexes for a given table",
      notes = "Get all indexes for a given table",
      response = Map.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Map.class),
        @ApiResponse(code = 400, message = "Bad request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error",
            response = RestServiceError.class)
      })
  Response getAllIndexesForTable(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Context HttpServletRequest request);

  @Timed
  @POST
  @ApiOperation(
      value = "Add an index to a table's column",
      notes = "Add an index to a single column of a table.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response addIndex(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(required = true) final Sgv2IndexAddRequest indexAdd,
      @Context HttpServletRequest request);

  @Timed
  @DELETE
  @ApiOperation(
      value = "Drop an index from keyspace",
      notes = "Drop an index",
      response = Map.class,
      code = 204)
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error",
            response = RestServiceError.class),
      })
  @Path("/{indexName}")
  Response dropIndex(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "Name of the index to use for the request.", required = true)
          @PathParam("indexName")
          final String indexName,
      @ApiParam(
              defaultValue = "false",
              value =
                  "If the index doesn't exists drop will throw an error unless this query param is set to true.")
          @QueryParam("ifExists")
          final boolean ifExists,
      @Context HttpServletRequest request);
}
