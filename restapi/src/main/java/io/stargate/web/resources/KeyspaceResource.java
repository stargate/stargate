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
package io.stargate.web.resources;

import com.codahale.metrics.annotation.Timed;
import io.stargate.db.datastore.DataStore;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
@Path("/v1/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
public class KeyspaceResource {

  private static final Logger logger = LoggerFactory.getLogger(KeyspaceResource.class);

  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "Return all keyspaces",
      nickname = "getKeyspaces",
      notes = "Retrieve all available keyspaces in the specific database.",
      response = String.class,
      responseContainer = "List",
      httpMethod = "GET",
      produces = "application/json",
      consumes = "application/json",
      tags = {
        "keyspaces",
      })
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 200,
            message = "OK",
            response = String.class,
            responseContainer = "List"),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  public Response listAll(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true,
              type = "string",
              format = "uuid")
          @HeaderParam("X-Cassandra-Token")
          String token) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);
          return Response.status(Response.Status.OK)
              .entity(localDB.schema().keyspaceNames())
              .build();
        });
  }
}
