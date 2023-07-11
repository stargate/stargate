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
package io.stargate.sgv2.restapi.service.resources.schemas;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
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
 * Definition of REST API DDL endpoint methods for Index access including JAX-RS and OpenAPI
 * annotations. No implementations.
 */
@ApplicationScoped
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/indexes")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = RestOpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = RestOpenApiConstants.Tags.SCHEMA)
public interface Sgv2IndexesResourceApi {
  @GET
  @Operation(
      summary = "Get all indexes for a given table",
      description = "Get all indexes for a given table")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(type = SchemaType.OBJECT))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  Uni<RestResponse<Object>> getAllIndexes(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(name = "compactMapData", ref = RestOpenApiConstants.Parameters.COMPACT_MAP_DATA)
          @QueryParam("compactMapData")
          final Boolean compactMapData);

  @POST
  @Operation(
      summary = "Add an index to a table's column",
      description = "Add an index to a single column of a table.")
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
  Uni<RestResponse<Map<String, Object>>> addIndex(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @RequestBody(description = "Index definition as JSON", required = true) @NotNull @Valid
          final Sgv2IndexAddRequest indexAdd);

  @DELETE
  @Operation(summary = "Drop an index from keyspace", description = "Drop an index")
  @APIResponses(
      value = {
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_204),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500)
      })
  @Path("/{indexName}")
  Uni<RestResponse<Void>> deleteIndex(
      @Parameter(name = "keyspaceName", ref = RestOpenApiConstants.Parameters.KEYSPACE_NAME)
          @PathParam("keyspaceName")
          @NotBlank(message = "keyspaceName must be provided")
          final String keyspaceName,
      @Parameter(name = "tableName", ref = RestOpenApiConstants.Parameters.TABLE_NAME)
          @PathParam("tableName")
          @NotBlank(message = "tableName must be provided")
          final String tableName,
      @Parameter(
              name = "Name of the index to use for the request",
              required = true,
              schema = @Schema(type = SchemaType.STRING))
          @PathParam("indexName")
          @NotBlank(message = "indexName must be provided")
          final String indexName,
      @Parameter(
              name =
                  "If the index doesn't exist drop will throw an error unless this query param is set to true",
              schema = @Schema(implementation = boolean.class))
          @QueryParam("ifExists")
          final boolean ifExists);
}
