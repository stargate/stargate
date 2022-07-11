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
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents.model.dto.BuiltInFunctionDto;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.function.FunctionService;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

/** Document write resource. */
@Path(DocumentWriteResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class BuiltInFunctionResource {

  public static final String BASE_PATH = "/v2/namespaces/{namespace:\\w+}/collections";

  @Inject FunctionService functionService;

  @Inject TableManager tableManager;

  @Operation(
      description =
          "Execute a built-in function (e.g. `$push` and `$pop`) against a value in this document. Performance may vary.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(name = "document-id", ref = OpenApiConstants.Parameters.DOCUMENT_ID),
        @Parameter(name = "document-path", ref = OpenApiConstants.Parameters.DOCUMENT_PATH),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @RequestBody(ref = OpenApiConstants.RequestBodies.WRITE_BUILT_IN_FUNCTION)
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Function is executed. Certain functions will return a value in the `data` property. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = DocumentResponseWrapper.class),
                  examples =
                      @ExampleObject(
                          ref = OpenApiConstants.Examples.DOCUMENT_SINGLE)) // TODO example
            }),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                      @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST),
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
  @Path("{collection:\\w+}/{document-id}/{document-path:.*}/function")
  public Uni<RestResponse<Object>> executeBuiltInFunction(
      @Context UriInfo uriInfo,
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String documentId,
      @PathParam("document-path") List<PathSegment> documentPath,
      @QueryParam("profile") boolean profile,
      @QueryParam("raw") boolean raw,
      @NotNull(message = "payload not provided") @Valid BuiltInFunctionDto body) {
    ExecutionContext context = ExecutionContext.create(profile);
    List<String> subPath =
        documentPath.stream().map(PathSegment::getPath).collect(Collectors.toList());

    // call table manager for valid table
    return tableManager
        .getValidCollectionTable(namespace, collection)

        // if there execute the function
        .flatMap(
            table ->
                functionService
                    .executeBuiltInFunction(
                        Uni.createFrom().item(table),
                        namespace,
                        collection,
                        documentId,
                        subPath,
                        body.operation(),
                        body.value(),
                        context)

                    // map with raw in mind
                    .map(
                        result -> {
                          // if null, means original doc was not found
                          if (null == result) {
                            String msg;
                            if (subPath.isEmpty()) {
                              msg =
                                  "A document with the id %s does not exist.".formatted(documentId);
                            } else {
                              msg =
                                  "A path %s in a document with the id %s, or the document itself, does not exist."
                                      .formatted(subPath, documentId);
                            }
                            int code = Response.Status.NOT_FOUND.getStatusCode();
                            ApiError error = new ApiError(msg, 404);
                            return RestResponse.ResponseBuilder.create(code).entity(error).build();
                          }

                          // otherwise map
                          if (raw) {
                            return RestResponse.ok(result.data());
                          } else {
                            return RestResponse.ok(result);
                          }
                        }));
  }
}
