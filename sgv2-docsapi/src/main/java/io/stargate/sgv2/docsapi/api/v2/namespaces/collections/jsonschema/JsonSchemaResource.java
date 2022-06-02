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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.JsonSchemaDto;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.schema.JsonSchemaManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

/** JSON Schema resource. */
@Path(JsonSchemaResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.JSON_SCHEMAS)
public class JsonSchemaResource {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace:\\w+}/collections/{collection:\\w+}/json-schema";

  @Inject @Authorized TableManager tableManager;

  @Inject JsonSchemaManager jsonSchemaManager;

  @Operation(
      description =
          "Assign a JSON schema to a collection. This will erase any schema that already exists.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace of the collection"),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection to assign a JSON schema to.")
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Call successful.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = JsonSchemaDto.class,
                          properties = @SchemaProperty(name = "schema", type = SchemaType.OBJECT)))
            }),
        @APIResponse(
            responseCode = "400",
            description = "Bad request.",
            content =
                @Content(
                    examples = {@ExampleObject(ref = "Invalid JSON schema.")},
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                      @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @PUT
  public Uni<RestResponse<Object>> putJsonSchema(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @NotNull(message = "json schema not provided") JsonNode body) {
    return jsonSchemaManager
        .attachJsonSchema(
            namespace, tableManager.getValidCollectionTable(namespace, collection), body)
        .map(schema -> RestResponse.ok(new JsonSchemaDto(schema)));
  }

  @Operation(description = "Get a JSON schema from a collection.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace of the collection"),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection to get the JSON schema from.")
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Fetch successful.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = JsonSchemaDto.class,
                          properties = @SchemaProperty(name = "schema", type = SchemaType.OBJECT)))
            }),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                      @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @GET
  public Uni<RestResponse<Object>> getJsonSchema(
      @Context UriInfo uriInfo,
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection) {

    // go get the existing table
    return jsonSchemaManager
        .getJsonSchema(tableManager.getValidCollectionTable(namespace, collection))
        .map(schema -> RestResponse.ok(new JsonSchemaDto(schema)));
  }
}
