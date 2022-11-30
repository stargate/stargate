package io.stargate.sgv2.restapi.service.resources.schemas;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2UDT;
import io.stargate.sgv2.restapi.service.models.Sgv2UDTUpdateRequest;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
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
 * Definition of REST API DDL endpoint for Cassandra User Defined Types access including JAX-RS and
 * OpenAPI annotations. No implementations.
 *
 * @see "https://cassandra.apache.org/doc/latest/cql/types.html"
 */
@ApplicationScoped
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = RestOpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = RestOpenApiConstants.Tags.SCHEMA)
public interface Sgv2UDTsResourceApi {
  @GET
  @Operation(
      summary = "Get all user defined types (UDT). ",
      description = "Retrieve all user defined types (UDT) in a specific keyspace.")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content =
                @Content(
                    schema = @Schema(implementation = Sgv2UDT.class, type = SchemaType.ARRAY))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_404),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  Uni<RestResponse<Object>> findAllTypes(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw);

  @GET
  @Operation(
      summary = "Get an user defined type (UDT) from its identifier",
      description = "Retrieve data for a single table in a specific keyspace")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = Sgv2UDT.class))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_404),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{typeName}")
  Uni<RestResponse<Object>> findTypeById(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(
              name = "typeName",
              description = "Name of the type to find",
              required = true,
              schema = @Schema(type = SchemaType.STRING))
          @PathParam("typeName")
          @NotBlank(message = "typeName must be provided")
          final String typeName,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw);

  @POST
  @Operation(
      summary = "Create an user defined type (UDT)",
      description = "Add an user defined type (udt) in a specific keyspace.")
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
  Uni<RestResponse<Map<String, String>>> createType(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @RequestBody(description = "Type definition as JSON", required = true) @NotNull
          final String udtAddPayload);

  @PUT
  @Operation(
      summary = "Update an User Defined type (UDT)",
      description = "Update an user defined type (UDT) adding or renaming fields.")
  @APIResponses(
      value = {
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_204),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  Uni<RestResponse<Void>> updateType(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @RequestBody(description = "Type definition as JSON", required = true) @NotNull @Valid
          final Sgv2UDTUpdateRequest udtUpdate);

  @DELETE
  @Operation(
      summary = "Delete an User Defined type (UDT)",
      description = "Delete a single user defined type (UDT) in the specified keyspace.")
  @APIResponses(
      value = {
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_204),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{typeName}")
  Uni<RestResponse<Void>> deleteType(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(
              name = "typeName",
              description = "Name of the type to delete",
              required = true,
              schema = @Schema(type = SchemaType.STRING))
          @PathParam("typeName")
          @NotBlank(message = "typeName must be provided")
          final String typeName);
}
