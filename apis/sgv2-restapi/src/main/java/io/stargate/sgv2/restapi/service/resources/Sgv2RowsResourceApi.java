package io.stargate.sgv2.restapi.service.resources;

import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.RestServiceError;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2RowsResponse;
import java.util.List;
import java.util.Map;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Definition of REST API endpoint methods including both JAX-RS and Swagger annotations. No
 * implementations.
 *
 * <p>NOTE: JAX-RS class annotations cannot be included in the interface and must be included in the
 * implementation class. Swagger annotations are ok tho.
 */
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(ref = RestOpenApiConstants.Tags.DATA)
public interface Sgv2RowsResourceApi {
  @GET
  @Operation(
      summary = "Search a table",
      description = "Search a table using a json query as defined in the `where` query parameter")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = Sgv2RowsResponse.class))),
        //      response = Sgv2RowsResponse.class,
        //      responseContainer = "List")
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  Response getRowWithWhere(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(
              description =
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
      @Parameter(description = "URL escaped, comma delimited list of keys to include")
          @QueryParam("fields")
          final String fields,
      @Parameter(description = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(description = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) final boolean raw,
      @Parameter(description = "Keys to sort by") @QueryParam("sort") final String sort);

  @GET
  @Operation(
      summary = "Get row(s)",
      description = "Get rows from a table based on the primary key.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = Sgv2RowsResponse.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .*}")
  Response getRows(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(
              description =
                  "Value from the primary key column for the table. Define composite keys by separating values"
                      + " with slashes (`val1/val2...`) in the order they were defined. </br>"
                      + "For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)`"
                      + " then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(description = "URL escaped, comma delimited list of keys to include")
          @QueryParam("fields")
          final String fields,
      @Parameter(description = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(description = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) final boolean raw,
      @Parameter(description = "Keys to sort by") @QueryParam("sort") final String sort);

  @GET
  @Operation(summary = "Retrieve all rows", description = "Get all rows from a table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = Sgv2RowsResponse.class))),
        @APIResponse(
            responseCode = "404",
            description = "Not Found",
            content = @Content(schema = @Schema(implementation = RestServiceError.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/rows")
  Response getAllRows(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @QueryParam("fields") String fields,
      @Parameter(description = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(description = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) final boolean raw,
      @Parameter(description = "Keys to sort by") @QueryParam("sort") final String sort);

  @POST
  @Operation(
      summary = "Add row",
      description =
          "Add a row to a table in your database. If the new row has the same primary key as that of"
              + " an existing row, the database processes it as an update to the existing row.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "201",
            description = "Resource created",
            content = @Content(schema = @Schema(implementation = Map.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  Response createRow(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(description = "", required = true) final String payloadAsString);

  @PUT
  @Operation(summary = "Replace row(s)", description = "Update existing rows in a table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "resource updated",
            content = @Content(schema = @Schema(implementation = Sgv2RESTResponse.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .*}")
  Response updateRows(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(
              description =
                  "Value from the primary key column for the table. Define composite keys by separating"
                      + " values with slashes (`val1/val2...`) in the order they were defined. </br>"
                      + "For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)`"
                      + " then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) final boolean raw,
      @Parameter(description = "", required = true) String payload);

  @DELETE
  @Operation(summary = "Delete row(s)", description = "Delete one or more rows in a table")
  @APIResponses(
      value = {
        @APIResponse(responseCode = "204", description = "No Content"),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .*}")
  Response deleteRows(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(
              description =
                  "Value from the primary key column for the table. Define composite keys by separating values"
                      + " with slashes (`val1/val2...`) in the order they were defined. </br>"
                      + "For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)`"
                      + " then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path);

  @PATCH
  @Operation(
      summary = "Update part of a row(s)",
      description = "Perform a partial update of one or more rows in a table")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Resource updated",
            content = @Content(schema = @Schema(implementation = Sgv2RESTResponse.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .*}")
  Response patchRows(
      @Parameter(description = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(description = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Parameter(
              description =
                  "Value from the primary key column for the table. Define composite keys by separating values"
                      + " with slashes (`val1/val2...`) in the order they were defined. </br>"
                      + "For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)`"
                      + " then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) final boolean raw,
      @Parameter(description = "document", required = true) String payload);
}
