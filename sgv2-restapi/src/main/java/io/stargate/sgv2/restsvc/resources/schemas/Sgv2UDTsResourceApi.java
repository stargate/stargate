package io.stargate.sgv2.restsvc.resources.schemas;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.Timed;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.models.Sgv2UDT;
import io.stargate.sgv2.restsvc.models.Sgv2UDTAddRequest;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * Exposes REST Endpoint to work with Cassandra User Defined Types
 *
 * @see "https://cassandra.apache.org/doc/latest/cql/types.html"
 */
@Api(
    produces = APPLICATION_JSON,
    consumes = APPLICATION_JSON,
    tags = {"schemas"})
@ApiImplicitParams({
  @ApiImplicitParam(
      name = "X-Cassandra-Token",
      paramType = "header",
      value = "The token returned from the authorization endpoint. Use this token in each request.",
      required = true)
})
public interface Sgv2UDTsResourceApi {
  static class UserDefinedTypeUpdate {}

  @Timed
  @GET
  @ApiOperation(
      value = "Get all user defined types (UDT). ",
      notes = "Retrieve all user defined types (UDT) in a specific keyspace.",
      response = Sgv2UDT.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2UDT.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 404,
            message = "Keyspace has not been found",
            response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response findAll(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Keyspace to find all udts", required = true) @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @GET
  @ApiOperation(
      value = "Get an user defined type (UDT) from its identifier",
      notes = "Retrieve data for a single table in a specific keyspace.",
      response = Sgv2RESTResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2RESTResponse.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{typeName}")
  Response findById(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(
              value = "Name of the user defined type (UDT) to use for the request.",
              required = true)
          @PathParam("typeName")
          final String typeName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @POST
  @ApiOperation(
      value = "Create an user defined type (UDT)",
      notes = "Add an user defined type (udt) in a specific keyspace.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(
            code = 400,
            message = "Bad Request, the input is not well formated",
            response = RestServiceError.class),
        @ApiResponse(
            code = 401,
            message = "Unauthorized, token is not valid or not enough permissions",
            response = RestServiceError.class),
        @ApiResponse(
            code = 409,
            message = "Conflict, the object may already exist",
            response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response createType(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "", required = true) @NotNull final Sgv2UDTAddRequest udtAdd,
      @Context final HttpServletRequest request);

  @Timed
  @DELETE
  @ApiOperation(
      value = "Delete an User Defined type (UDT)",
      notes = "Delete a single user defined type (UDT) in the specified keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{typeName}")
  Response delete(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(
              value = "Name of the user defined type (UDT) to use for the request.",
              required = true)
          @PathParam("typeName")
          final String typeName,
      @Context HttpServletRequest request);

  @Timed
  @PUT
  @ApiOperation(
      value = "Update an User Defined type (UDT)",
      notes = "Update an user defined type (UDT) adding or renaming fields.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response update(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "", required = true) @NotNull final UserDefinedTypeUpdate udtUpdate,
      @Context HttpServletRequest request);
}
