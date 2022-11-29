package io.stargate.sgv2.restapi.service.resources.schemas;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
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
 * Definition of REST API DDL endpoint methods for Columns access including JAX-RS and OpenAPI
 * annotations. No implementations.
 */
@ApplicationScoped
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = RestOpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = RestOpenApiConstants.Tags.SCHEMA)
public interface Sgv2ColumnsResourceApi {
  @GET
  @Operation(summary = "Get all columns", description = "Return all columns for a specified table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content =
                @Content(
                    schema =
                        @Schema(
                            implementation = Sgv2ColumnDefinition.class,
                            type = SchemaType.ARRAY))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_404),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  Uni<RestResponse<Object>> getAllColumns(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          final String tableName,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw);

  @POST
  @Operation(summary = "Create a column", description = "Add a single column to a table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "201",
            description = "Created",
            content = @Content(schema = @Schema(type = SchemaType.OBJECT))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  Uni<RestResponse<Map<String, String>>> createColumn(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          final String tableName,
      @RequestBody(description = "Column definition as JSON", required = true) @NotNull
          final Sgv2ColumnDefinition columnDefinition);

  @GET
  @Operation(
      summary = "Get a column",
      description = "Return a single column specification in a specific table.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = Sgv2ColumnDefinition.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_404),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{columnName}")
  Uni<RestResponse<Object>> getOneColumn(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          final String tableName,
      @Parameter(name = "columnName", required = true) @PathParam("columnName")
          final String columnName,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw);

  @PUT
  @Operation(
      summary = "Update a column",
      description = "Update a single column in a specific table")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Resource updated",
            content = @Content(schema = @Schema(type = SchemaType.OBJECT))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_404),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{columnName}")
  Uni<RestResponse<Map<String, String>>> updateColumn(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          final String tableName,
      @PathParam("columnName") final String columnName,
      @NotNull final Sgv2ColumnDefinition columnUpdate);

  @DELETE
  @Operation(
      summary = "Delete a column",
      description = "Delete a single column in a specific table.")
  @APIResponses(
      value = {
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_204),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{columnName}")
  public Uni<RestResponse<Void>> deleteColumn(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          final String tableName,
      @Parameter(name = "column name", required = true) @PathParam("columnName")
          final String columnName);
}
