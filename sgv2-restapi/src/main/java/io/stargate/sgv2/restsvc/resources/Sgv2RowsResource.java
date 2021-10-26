package io.stargate.sgv2.restsvc.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2Rows;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2RowsResource {
  private final int DEFAULT_PAGE_SIZE = 100;

  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Channel used to connect to backend gRPC service. */
  @Inject private ManagedChannel grpcChannel;

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve all rows",
      notes = "Get all rows from a table.",
      response = Sgv2Rows.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Rows.class),
        @ApiResponse(code = 400, message = "Bad request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error",
            response = RestServiceError.class)
      })
  @Path("/rows")
  public javax.ws.rs.core.Response getAllRows(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @QueryParam("fields") String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort,
      @Context HttpServletRequest request) {
    StargateGrpc.StargateBlockingStub blockingStub =
        StargateGrpc.newBlockingStub(grpcChannel)
            .withCallCredentials(new StargateBearerToken(token))
            .withDeadlineAfter(5, TimeUnit.SECONDS);
    if (isStringEmpty(fields)) {
        fields = "*";
    }
    final String cql = String.format("SELECT %s from %s.%s", fields, keyspaceName, tableName);
    logger.info("Calling gRPC method: try to call backend with CQL of '" + cql + "'");
    QueryOuterClass.Response response =
         blockingStub.executeQuery(QueryOuterClass.Query.newBuilder().setCql(cql).build());
    QueryOuterClass.ResultSet rs;
      try {
          rs = response.getResultSet().getData().unpack(QueryOuterClass.ResultSet.class);
      } catch (InvalidProtocolBufferException e) {
          return handleGrpcDecodeError(cql, e);
      }
      logger.info(
        "Calling gRPC method: response == "
            + new String(response.toByteArray(), StandardCharsets.UTF_8));
    return javax.ws.rs.core.Response.status(Response.Status.OK).entity(response).build();
  }

  private javax.ws.rs.core.Response handleGrpcDecodeError(String cql, InvalidProtocolBufferException e)
  {
      final String msg = String.format("Problem decoding Protobuf content for CQL query '%s': %s",
              cql, e.getMessage());
      logger.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity(new RestServiceError(msg, Response.Status.BAD_REQUEST.getStatusCode()))
              .build();
  }

  // So we won't need Guava dependency; may be moved to our own util if needed elsewhere
  private static boolean isStringEmpty(String str) {
      return (str == null) || str.isEmpty();
  }
}
