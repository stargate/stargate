package io.stargate.sgv2.restapi.service.resources;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2RowsResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.constraints.NotBlank;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.PathSegment;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Definition of REST API DML endpoint methods including JAX-RS and OpenAPI annotations. No
 * implementations.
 */
@ApplicationScoped
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Encoded
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = RestOpenApiConstants.SecuritySchemes.TOKEN)
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
            content =
                @Content(
                    schema =
                        @Schema(implementation = Sgv2RowsResponse.class, type = SchemaType.ARRAY))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  Uni<RestResponse<Object>> getRowWithWhere(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(
              in = ParameterIn.QUERY,
              name = "where",
              description =
                  "JSON query using the following keys: \n "
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
              schema = @Schema(type = SchemaType.OBJECT),
              required = true)
          @QueryParam("where")
          @NotBlank(message = "'where' must be provided")
          final String where,
      @Parameter(name = "fields", ref = RestOpenApiConstants.Parameters.FIELDS)
          @QueryParam("fields")
          final String fields,
      @Parameter(name = "page-size", ref = RestOpenApiConstants.Parameters.PAGE_SIZE)
          @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(name = "page-state", ref = RestOpenApiConstants.Parameters.PAGE_STATE)
          @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @Parameter(name = "sort", ref = RestOpenApiConstants.Parameters.SORT) @QueryParam("sort")
          final String sort);

  @GET
  @Operation(
      summary = "Get row(s)",
      description = "Get rows from a table based on the primary key.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content =
                @Content(
                    schema =
                        @Schema(implementation = Sgv2RowsResponse.class, type = SchemaType.ARRAY))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .+}")
  Uni<RestResponse<Object>> getRows(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "primaryKey", ref = RestOpenApiConstants.Parameters.PRIMARY_KEY)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(name = "fields", ref = RestOpenApiConstants.Parameters.FIELDS)
          @QueryParam("fields")
          final String fields,
      @Parameter(name = "page-size", ref = RestOpenApiConstants.Parameters.PAGE_SIZE)
          @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(name = "page-state", ref = RestOpenApiConstants.Parameters.PAGE_STATE)
          @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @Parameter(name = "sort", ref = RestOpenApiConstants.Parameters.SORT) @QueryParam("sort")
          final String sort);

  @GET
  @Operation(summary = "Retrieve all rows", description = "Get all rows from a table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content =
                @Content(
                    schema =
                        @Schema(implementation = Sgv2RowsResponse.class, type = SchemaType.ARRAY))),
        @APIResponse(
            responseCode = "404",
            description = "Not Found",
            content = @Content(schema = @Schema(implementation = ApiError.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/rows")
  Uni<RestResponse<Object>> getAllRows(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "fields", ref = RestOpenApiConstants.Parameters.FIELDS)
          @QueryParam("fields")
          String fields,
      @Parameter(name = "page-size", ref = RestOpenApiConstants.Parameters.PAGE_SIZE)
          @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(name = "page-state", ref = RestOpenApiConstants.Parameters.PAGE_STATE)
          @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @Parameter(name = "sort", ref = RestOpenApiConstants.Parameters.SORT) @QueryParam("sort")
          final String sort);

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
            content = @Content(schema = @Schema(type = SchemaType.OBJECT))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  Uni<RestResponse<Object>> createRow(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @RequestBody(description = "Fields of the Row to create as JSON", required = true)
          final String payloadAsString);

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
  @Path("/{primaryKey: .+}")
  Uni<RestResponse<Object>> updateRows(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "primaryKey", ref = RestOpenApiConstants.Parameters.PRIMARY_KEY)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @RequestBody(description = "Fields of the Row to update as JSON", required = true)
          String payloadAsString);

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
  @Path("/{primaryKey: .+}")
  Uni<RestResponse<Object>> patchRows(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "primaryKey", ref = RestOpenApiConstants.Parameters.PRIMARY_KEY)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @RequestBody(description = "Fields of the Row to patch as JSON", required = true)
          String payloadAsString);

  @DELETE
  @Operation(summary = "Delete row(s)", description = "Delete one or more rows in a table")
  @APIResponses(
      value = {
        @APIResponse(responseCode = "204", description = "No Content"),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  @Path("/{primaryKey: .+}")
  Uni<RestResponse<Object>> deleteRows(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "primaryKey", ref = RestOpenApiConstants.Parameters.PRIMARY_KEY)
          @PathParam("primaryKey")
          List<PathSegment> path);
}
