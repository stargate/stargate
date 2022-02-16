package io.stargate.sgv2.docssvc.resources;

import com.codahale.metrics.annotation.Timed;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.models.Sgv2RowsResponse;
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
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Definition of REST API endpoint methods including both JAX-RS and Swagger annotations. No
 * implementations.
 *
 * <p>NOTE: JAX-RS class annotations cannot be included in the interface and must be included in the
 * implementation class. Swagger annotations are ok tho.
 */
@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@ApiImplicitParams({
  @ApiImplicitParam(
      name = "X-Cassandra-Token",
      paramType = "header",
      value = "The token returned from the authorization endpoint. Use this token in each request.",
      required = true)
})
public interface Sgv2SearchDocsResourceApi {
  @Timed
  @GET
  @ApiOperation(
      value = "Search a collection",
      notes = "Search a collection using a json query as defined in the `where` query parameter",
      response = Sgv2RowsResponse.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2RowsResponse.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  Response searchCollection(
      @Context StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      @ApiParam(value = "Name of the namespace to use for the request.", required = true)
          @PathParam("namespace-id")
          final String namespace,
      @ApiParam(value = "Name of the collection to use for the request.", required = true)
          @PathParam("collection-id")
          final String collection,
      @ApiParam(
              value =
                  "URL escaped JSON query using the following keys: \n "
                      + "| Key | Operation | \n "
                      + "|-|-| \n "
                      + "| $lt | Less Than | \n "
                      + "| $lte | Less Than Or Equal To | \n "
                      + "| $gt | Greater Than | \n "
                      + "| $gte | Greater Than Or Equal To | \n "
                      + "| $eq | Equal To | \n "
                      + "| $ne | Not Equal To | \n "
                      + "| $in | Contained In | \n "
                      + "| $contains | Contains the given element (for lists or sets) or value (for maps) | \n "
                      + "| $containsKey | Contains the given key (for maps) | \n "
                      + "| $containsEntry | Contains the given key/value entry (for maps) | \n "
                      + "| $exists | Returns the rows whose column (boolean type) value is true | ",
              required = true)
          @QueryParam("where")
          final String where,
      @ApiParam(value = "URL escaped, comma delimited list of keys to include")
          @QueryParam("fields")
          final String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request);
}
