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
import io.stargate.sgv2.docsapi.api.v2.model.dto.ExecutionProfile;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.schema.CollectionManager;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
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

/** Document update resource. */
@Path(DocumentUpdateResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentUpdateResource {

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject WriteDocumentsService documentWriteService;

  @Inject CollectionManager collectionManager;

  @Operation(
      summary = "Create or update a document",
      description =
          "Create or update a document with a given ID. If the collection does not exist, it will be created. If the document already exists, it will be overwritten.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection of the document. Will be created if it does not exist."),
        @Parameter(
            name = "document-id",
            ref = OpenApiConstants.Parameters.DOCUMENT_ID,
            description = "The ID of the document to update."),
        @Parameter(name = "ttl", ref = OpenApiConstants.Parameters.TTL),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.WRITE)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Document updated or created with the provided document-id. The document-id will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          properties = {
                            @SchemaProperty(
                                name = "documentId",
                                type = SchemaType.STRING,
                                description = "The ID of the updated document."),
                            @SchemaProperty(
                                name = "profile",
                                implementation = ExecutionProfile.class,
                                nullable = true),
                          }),
                  examples = @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_WRITE))
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
  @PUT
  @Path("{collection:\\w+}/{document-id}")
  public Uni<RestResponse<Object>> updateDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @QueryParam("ttl") @Min(value = 1, message = "TTL value must be a positive integer")
          Integer ttl,
      @QueryParam("profile") boolean profile,
      @NotNull(message = "payload must not be empty") JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    Uni<Schema.CqlTable> table = collectionManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .updateDocument(table, namespace, collection, documentId, body, ttl, context)
        .onItem()
        .transform(result -> RestResponse.ResponseBuilder.ok().entity(result).build());
  }

  @Operation(
      summary = "Create or update a path in a document",
      description =
          "Create or update a path in a document by ID. If the collection does not exist, it will be created. If data exists at the path, it will be overwritten.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection of the document. Will be created if it does not exist."),
        @Parameter(
            name = "document-id",
            ref = OpenApiConstants.Parameters.DOCUMENT_ID,
            description = "The ID of the document to update."),
        @Parameter(name = "document-path", ref = OpenApiConstants.Parameters.DOCUMENT_PATH),
        @Parameter(name = "ttl-auto", ref = OpenApiConstants.Parameters.TTL_AUTO),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.WRITE_SUB_DOCUMENT)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Document updated or created with the provided document-id. The document-id will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          properties = {
                            @SchemaProperty(
                                name = "documentId",
                                type = SchemaType.STRING,
                                description = "The ID of the updated document."),
                            @SchemaProperty(
                                name = "profile",
                                implementation = ExecutionProfile.class,
                                nullable = true),
                          }),
                  examples = @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_WRITE))
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
  @PUT
  @Path("{collection:\\w+}/{document-id}/{document-path:.*}")
  public Uni<RestResponse<Object>> updateSubDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @PathParam("document-path") List<PathSegment> documentPath,
      @QueryParam("ttl-auto") boolean ttlAuto,
      @QueryParam("profile") boolean profile,
      @NotNull(message = "payload must not be empty") JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    List<String> subPath =
        documentPath.stream().map(PathSegment::getPath).collect(Collectors.toList());
    Uni<Schema.CqlTable> table = collectionManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .updateSubDocument(
            table, namespace, collection, documentId, subPath, body, ttlAuto, context)
        .onItem()
        .transform(result -> RestResponse.ResponseBuilder.ok().entity(result).build());
  }
}
