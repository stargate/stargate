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
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.PATCH;
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

/** Document patch resource. */
@Path(DocumentPatchResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentPatchResource {

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject WriteDocumentsService documentWriteService;

  @Inject TableManager tableManager;

  @Operation(
      description =
          "Patch a document with a given ID. Merges data at the root with requested data. Note that operation is not allowed if a JSON schema exist for a target collection.")
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
            description = "The ID of the document that you'd like to patch"),
        @Parameter(name = "ttl-auto", ref = OpenApiConstants.Parameters.TTL_AUTO),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.PATCH)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Document patched. The document-id will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          properties = {
                            @SchemaProperty(
                                name = "documentId",
                                type = SchemaType.STRING,
                                description = "The ID of the patched document."),
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
  @PATCH
  @Path("{collection:\\w+}/{document-id}")
  public Uni<RestResponse<Object>> patchDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @QueryParam("ttl-auto") boolean ttlAuto,
      @QueryParam("profile") boolean profile,
      @NotNull JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    Uni<Schema.CqlTable> table = tableManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .patchDocument(table, namespace, collection, documentId, body, ttlAuto, context)
        .onItem()
        .transform(result -> RestResponse.ResponseBuilder.ok().entity(result).build());
  }

  @Operation(
      description =
          "Patch data at a path in a document by ID. Merges data at the path with requested data, assumes that the data at the path is already an object. Note that operation is not allowed if a JSON schema exist for a target collection.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace to write the document to."),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection of the document. Will be created if it does not exist."),
        @Parameter(
            name = "document-id",
            ref = OpenApiConstants.Parameters.DOCUMENT_ID,
            description = "The ID of the document that you'd like to write"),
        @Parameter(
            name = "document-path",
            ref = OpenApiConstants.Parameters.DOCUMENT_PATH,
            description = "The path within the document you would like to write to"),
        @Parameter(
            name = "ttl-auto",
            ref = OpenApiConstants.Parameters.TTL_AUTO,
            description =
                "Include this to make the TTL match that of the parent document. Requires read-before-write if set to true"),
        @Parameter(
            name = "profile",
            ref = OpenApiConstants.Parameters.PROFILE,
            description = "Include profiling information from execution."),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.PATCH)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "Document path patched. The document-id will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          properties = {
                            @SchemaProperty(
                                name = "documentId",
                                type = SchemaType.STRING,
                                description = "The ID of the patched document."),
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
  @PATCH
  @Path("{collection:\\w+}/{document-id}/{document-path:.*}")
  public Uni<RestResponse<Object>> patchSubDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @PathParam("document-path") List<PathSegment> documentPath,
      @QueryParam("ttl-auto") boolean ttlAuto,
      @QueryParam("profile") boolean profile,
      @NotNull JsonNode body) {
    ExecutionContext context = ExecutionContext.create(profile);
    List<String> subPath =
        documentPath.stream().map(PathSegment::getPath).collect(Collectors.toList());
    Uni<Schema.CqlTable> table = tableManager.ensureValidDocumentTable(namespace, collection);
    return documentWriteService
        .patchSubDocument(table, namespace, collection, documentId, subPath, body, ttlAuto, context)
        .onItem()
        .transform(result -> RestResponse.ResponseBuilder.ok().entity(result).build());
  }
}
