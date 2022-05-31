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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.SimpleResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.CollectionDto;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.CollectionUpgradeType;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.CreateCollectionDto;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collections resource. */
@Path(CollectionsResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.COLLECTIONS)
public class CollectionsResource {

  private static final Logger LOG = LoggerFactory.getLogger(CollectionsResource.class);

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject @Authorized TableManager tableManager;

  @Inject DataStoreProperties dataStoreProperties;

  @Operation(description = "List collections in a namespace.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace to fetch collections for."),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents fof the `data` property.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = SimpleResponseWrapper.class,
                          properties =
                              @SchemaProperty(
                                  name = "data",
                                  type = SchemaType.ARRAY,
                                  implementation = CollectionDto.class,
                                  uniqueItems = true,
                                  minItems = 0)))
            }),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @GET
  public Uni<RestResponse<Object>> getCollections(
      @PathParam("namespace") String namespace, @QueryParam("raw") boolean raw) {

    // get all valid collection tables
    return tableManager
        .getValidCollectionTables(namespace)

        // map to DTO and collect list
        .map(this::toCollectionDto)
        .collect()
        .asList()

        // map to wrapper if needed
        .map(
            results -> {
              if (raw) {
                return results;
              } else {
                return new SimpleResponseWrapper<>(results);
              }
            })
        .map(RestResponse::ok);
  }

  @Operation(description = "Create a new empty collection in a namespace.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace to create the collection in."),
      })
  @APIResponses(
      value = {
        @APIResponse(responseCode = "201", description = "Collection created."),
        @APIResponse(
            responseCode = "400",
            description = "Bad request.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(
                          name = "Invalid collection name",
                          value =
                              """
                              {
                                  "code": 400,
                                  "description": "Could not create collection events-collection, it has invalid characters. Valid characters are alphanumeric and underscores."
                              }
                              """),
                      @ExampleObject(ref = OpenApiConstants.Examples.GENERAL_BAD_REQUEST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(
            responseCode = "409",
            description = "Conflict.",
            content =
                @Content(
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class,
                            example =
                                """
                                {
                                    "code": 409,
                                    "description": "Create failed: collection cycling already exists."
                                }
                                """))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @POST
  public Uni<RestResponse<Object>> createCollection(
      @Context UriInfo uriInfo,
      @PathParam("namespace") String namespace,
      @NotNull(message = "payload not provided") @Valid CreateCollectionDto body) {

    String collection = body.name();

    // go get the existing table
    return tableManager
        .getValidCollectionTable(namespace, collection)

        // in case there is a table, ensure we report as error
        .map(
            table -> {
              int code = Response.Status.CONFLICT.getStatusCode();
              ApiError error =
                  new ApiError(
                      "Create failed: collection %s already exists.".formatted(collection), code);
              return RestResponse.ResponseBuilder.create(code).entity(error).build();
            })

        // otherwise if table does not exist, table manager would fire the error
        // with DATASTORE_TABLE_DOES_NOT_EXIST
        .onFailure(
            t -> {
              if (t instanceof ErrorCodeRuntimeException ec) {
                return Objects.equals(ec.getErrorCode(), ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST);
              }
              return false;
            })
        .recoverWithUni(
            () ->
                tableManager
                    .createCollectionTable(namespace, collection)
                    .map(
                        created -> {
                          URI location = uriInfo.getBaseUriBuilder().path(collection).build();
                          return RestResponse.created(location);
                        }));
  }

  @Operation(description = "Delete a collection in a namespace.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION)
      })
  @APIResponses(
      value = {
        @APIResponse(responseCode = "204", description = "Collection deleted."),
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
  @DELETE
  @Path("/{collection:\\w+}")
  public Uni<RestResponse<Object>> deleteCollection(
      @PathParam("namespace") String namespace, @PathParam("collection") String collection) {
    // go delete the table
    return tableManager
        .dropCollectionTable(namespace, collection)
        .map(any -> RestResponse.noContent());
  }

  // simple mapper of CqlTable to the response DTO
  private CollectionDto toCollectionDto(Schema.CqlTable table) {
    String name = table.getName();

    // if sai enabled, check if they are all there
    if (dataStoreProperties.saiEnabled()) {
      // these are already only secondary indexes
      // see io.stargate.bridge.service.SchemaHandler#buildSecondaryIndex
      List<Schema.CqlIndex> indexes = table.getIndexesList();
      boolean noCustom = indexes.stream().noneMatch(Schema.CqlIndex::getCustom);

      // If all secondary indexes are not SAI or there are no secondary indexes,
      // then an upgrade is available.
      if (indexes.size() == 0 || noCustom) {
        new CollectionDto(name, true, CollectionUpgradeType.SAI_INDEX_UPGRADE);
      }
    }

    // no upgrade
    return new CollectionDto(name, false, null);
  }
}
