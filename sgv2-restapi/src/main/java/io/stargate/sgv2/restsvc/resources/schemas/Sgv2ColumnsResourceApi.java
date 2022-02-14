package io.stargate.sgv2.restsvc.resources.schemas;

import com.codahale.metrics.annotation.Timed;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
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
public interface Sgv2ColumnsResourceApi {
  @Timed
  @GET
  @ApiOperation(
      value = "Get all columns",
      notes = "Return all columns for a specified table.",
      response = Sgv2ColumnDefinition.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2ColumnDefinition.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response getAllColumns(
      @Context StargateBridgeClient bridge,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @POST
  @ApiOperation(
      value = "Create a column",
      notes = "Add a single column to a table.",
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
  Response createColumn(
      @Context StargateBridgeClient bridge,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "", required = true) @NotNull final Sgv2ColumnDefinition columnDefinition,
      @Context HttpServletRequest request);

  @Timed
  @GET
  @ApiOperation(
      value = "Get a column",
      notes = "Return a single column specification in a specific table.",
      response = Sgv2ColumnDefinition.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2ColumnDefinition.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{columnName}")
  Response getOneColumn(
      @Context StargateBridgeClient bridge,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "column name", required = true) @PathParam("columnName")
          final String columnName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);

  @Timed
  @PUT
  @ApiOperation(
      value = "Update a column",
      notes = "Update a single column in a specific table.",
      response = Map.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Map.class),
        @ApiResponse(code = 400, message = "Bad request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error",
            response = RestServiceError.class)
      })
  @Path("/{columnName}")
  Response updateColumn(
      @Context StargateBridgeClient bridge,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @PathParam("columnName") final String columnName,
      @NotNull final Sgv2ColumnDefinition columnUpdate,
      @Context HttpServletRequest request);

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a column", notes = "Delete a single column in a specific table.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{columnName}")
  public Response deleteColumn(
      @Context StargateBridgeClient bridge,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "column name", required = true) @PathParam("columnName")
          final String columnName,
      @Context HttpServletRequest request);
}
