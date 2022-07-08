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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.v2.model.dto.MultiDocsResponse;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.schema.CollectionManager;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import java.net.URI;
import javax.inject.Inject;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;

/** Document write resource. */
@Path(DocumentWriteResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentWriteResource {

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject WriteDocumentsService documentWriteService;

  @Inject CollectionManager collectionManager;

  @Operation(
      summary = "Create a document",
      description =
          "Create a new document with the generated ID. If the collection does not exist, it will be created.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection of the document. Will be created if it does not exist."),
        @Parameter(name = "ttl", ref = OpenApiConstants.Parameters.TTL),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.WRITE)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "201",
            description = "Document created. The ID will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          properties = {
                            @SchemaProperty(
                                name = "documentId",
                                type = SchemaType.STRING,
                                description = "The ID of the written document."),
                            @SchemaProperty(
                                name = "profile",
                                implementation = ExecutionProfile.class,
                                nullable = true),
                          }),
                  examples = @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_WRITE))
            },
            headers =
                @Header(
                    name = HttpHeaders.LOCATION,
                    description = "The URL of a newly created document.")),
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
  @POST
  @Path("{collection:\\w+}")
  public Uni<RestResponse<Object>> createDocument(
      @Context UriInfo uriInfo,
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @QueryParam("ttl") @Min(value = 1, message = "TTL value must be a positive integer")
          Integer ttl,
      @QueryParam("profile") boolean profile,
      @NotNull(message = "payload must not be empty") JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    Uni<Schema.CqlTable> table = collectionManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .writeDocument(table, namespace, collection, body, ttl, context)
        .onItem()
        .transform(
            result -> {
              URI location =
                  uriInfo
                      .getBaseUriBuilder()
                      .path(uriInfo.getPath())
                      .path(result.documentId())
                      .build();
              return ResponseBuilder.created(location).entity(result).build();
            });
  }

  @Operation(
      summary = "Create documents",
      description =
          """
              Create multiple new documents. If the collection does not exist, it will be created.

              > Include the `id-path` parameter to extract the ID for each document from the document itself.
              The `id-path` should be given as path to the document property containing the id, for example `a.b.c.[0]`.
              Note that document IDs will be auto-generated     in case there is no `id-path` parameter defined.
              """)
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection of the documents. Will be created if it does not exist."),
        @Parameter(
            name = "id-path",
            description =
                "The optional path of the ID in each document whose value will be used as the ID of the created document, if present."),
        @Parameter(
            name = "ttl",
            ref = OpenApiConstants.Parameters.TTL,
            description = "The time-to-live (in seconds) of each written document."),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.WRITE_BATCH)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "202",
            description = "Accepted. Writes will be processed, and ID's will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = MultiDocsResponse.class),
                  examples = @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_WRITE_BATCH))
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
  @POST
  @Path("{collection:\\w+}/batch")
  public Uni<RestResponse<Object>> createDocuments(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @QueryParam("id-path") String idPath,
      @QueryParam("ttl") @Min(value = 1, message = "TTL value must be a positive integer")
          Integer ttl,
      @QueryParam("profile") boolean profile,
      @NotNull(message = "payload must not be empty") JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    Uni<Schema.CqlTable> table = collectionManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .writeDocuments(table, namespace, collection, body, idPath, ttl, context)
        .onItem()
        .transform(result -> ResponseBuilder.accepted().entity(result).build());
  }
}
