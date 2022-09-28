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

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.schema.CollectionManager;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

/** Document delete resource. */
@Path(DocumentDeleteResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentDeleteResource {

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject WriteDocumentsService documentWriteService;

  @Inject CollectionManager collectionManager;

  @Operation(summary = "Delete a document", description = "Delete a document with a given ID.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(
            name = "document-id",
            ref = OpenApiConstants.Parameters.DOCUMENT_ID,
            description = "The ID of the document to delete."),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "204",
            description = "Document deleted successfully, or was never present."),
        @APIResponse(
            responseCode = "404",
            description = "Namespace or collection not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                      @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @DELETE
  @Path("{collection:\\w+}/{document-id}")
  public Uni<RestResponse<Object>> deleteDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @QueryParam("profile") boolean profile) {
    ExecutionContext context = ExecutionContext.create(profile);
    return collectionManager
        .getValidCollectionTable(namespace, collection)
        .onItem()
        .transformToUni(
            __ ->
                documentWriteService
                    .deleteDocument(namespace, collection, documentId, context)
                    .onItem()
                    .transform(result -> RestResponse.ResponseBuilder.noContent().build()));
  }

  @Operation(
      summary = "Delete a path in a document",
      description = "Delete the data at a path in a document by ID.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(
            name = "document-id",
            ref = OpenApiConstants.Parameters.DOCUMENT_ID,
            description = "The ID of the document to delete."),
        @Parameter(name = "document-path", ref = OpenApiConstants.Parameters.DOCUMENT_PATH),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "204",
            description =
                "Document updated or created with the provided document-id. The document-id will be returned."),
        @APIResponse(
            responseCode = "404",
            description = "Namespace or collection not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                      @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @DELETE
  @Path("{collection:\\w+}/{document-id}/{document-path:.*}")
  public Uni<RestResponse<Object>> deleteSubDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @PathParam("document-path") List<PathSegment> documentPath,
      @QueryParam("profile") boolean profile) {
    ExecutionContext context = ExecutionContext.create(profile);
    List<String> subPath =
        documentPath.stream().map(PathSegment::getPath).collect(Collectors.toList());
    return collectionManager
        .getValidCollectionTable(namespace, collection)
        .onItem()
        .transformToUni(
            __ ->
                documentWriteService
                    .deleteDocument(namespace, collection, documentId, subPath, context)
                    .onItem()
                    .transform(result -> RestResponse.ResponseBuilder.noContent().build()));
  }
}
