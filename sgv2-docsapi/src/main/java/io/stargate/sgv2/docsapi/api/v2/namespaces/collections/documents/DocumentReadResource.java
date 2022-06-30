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
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.ReadDocumentsService;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

/** Read resource. */
@Path(DocumentReadResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentReadResource {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace:\\w+}/collections/{collection:\\w+}";

  @Inject ReadDocumentsService readDocumentsService;

  @Inject TableManager tableManager;

  @Operation(
      summary = "Search documents in a collection",
      description = " Page over documents in a collection, with optional search parameters.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(name = "where", ref = OpenApiConstants.Parameters.WHERE),
        @Parameter(name = "fields", ref = OpenApiConstants.Parameters.FIELDS),
        @Parameter(
            name = "page-size",
            in = ParameterIn.QUERY,
            description = "The max number of results to return.",
            schema =
                @Schema(
                    implementation = Integer.class,
                    defaultValue = "3",
                    minimum = "1",
                    maximum = "20")),
        @Parameter(name = "page-state", ref = OpenApiConstants.Parameters.PAGE_STATE),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description =
            "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      name = "SearchDocsResult",
                      properties = {
                        @SchemaProperty(
                            name = "data",
                            type = SchemaType.ARRAY,
                            minItems = 0,
                            example = "[]"),
                        @SchemaProperty(
                            name = "pageState",
                            type = SchemaType.STRING,
                            example = "c29tZS1leGFtcGxlLXN0YXRl"),
                        @SchemaProperty(name = "profile", implementation = ExecutionProfile.class)
                      }),
              examples = {
                @ExampleObject(ref = OpenApiConstants.Examples.SEARCH_DOCUMENTS),
                @ExampleObject(ref = OpenApiConstants.Examples.SEARCH_DOCUMENTS_UNWRAPPED)
              })
        }),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
    @APIResponse(
        responseCode = "404",
        description = "Not found.",
        content =
            @Content(
                examples = {
                  @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                  @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST)
                },
                schema = @Schema(implementation = ApiError.class))),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
  })
  @GET
  public Uni<RestResponse<Object>> searchDocuments(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of results to return is one")
          @Max(value = 20, message = "the max number of documents to return is 20")
          Integer pageSize,
      @QueryParam("page-state") String pageState,
      @QueryParam("profile") boolean profile,
      @QueryParam("raw") boolean raw) {

    // fetch a valid table to ensure read is from a collection table
    return tableManager
        .getValidCollectionTable(namespace, collection)

        // if exists, then map to the read action
        .flatMap(
            t -> {
              ExecutionContext context = ExecutionContext.create(profile);

              // default page size of 3
              int pageSizeFinal = Optional.ofNullable(pageSize).orElse(3);
              Paginator paginator = new Paginator(pageState, pageSizeFinal);

              return readDocumentsService
                  .findDocuments(namespace, collection, where, fields, paginator, context)

                  // note that find documents always returns the result
                  .map(rawHandler(raw));
            });
  }

  @Operation(
      summary = "Get a document",
      description =
          """
          Retrieve the JSON representation of a single document.

          > Note that in case when conditions are given using the `where` query parameter, the response will contain an array of sub-documents where the condition is matched.
          The structure of returned sub-documents will only contain the path to the field which was included in the condition.
          Only single field conditions are possible at the moment. Multiple conditions targeting the same field are allowed.
          Mixing the `field` selection and `where` is not supported at the moment.
          The page size and page state parameters are only used together with `where` and enable paging through matched sub-documents.
          """)
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(name = "document-id", ref = OpenApiConstants.Parameters.DOCUMENT_ID),
        @Parameter(name = "where", ref = OpenApiConstants.Parameters.WHERE),
        @Parameter(name = "fields", ref = OpenApiConstants.Parameters.FIELDS),
        @Parameter(
            name = "page-size",
            in = ParameterIn.QUERY,
            description = "The max number of results to return.",
            schema = @Schema(implementation = Integer.class, defaultValue = "100", minimum = "1")),
        @Parameter(name = "page-state", ref = OpenApiConstants.Parameters.PAGE_STATE),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description =
            "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      implementation = DocumentResponseWrapper.class,
                      properties = @SchemaProperty(name = "data", type = SchemaType.DEFAULT)),
              examples = {
                @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_SINGLE),
                @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_SINGLE_UNWRAPPED),
                @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_SINGLE_WITH_WHERE),
              })
        }),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
    @APIResponse(
        responseCode = "404",
        description = "Not found.",
        content =
            @Content(
                examples = {
                  @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                  @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST),
                  @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_DOES_NOT_EXIST),
                },
                schema = @Schema(implementation = ApiError.class))),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
  })
  @GET
  @Path("{document-id}")
  public Uni<RestResponse<Object>> getDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String id,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of results to return is one")
          Integer pageSize,
      @QueryParam("page-state") String pageState,
      @QueryParam("profile") boolean profile,
      @QueryParam("raw") boolean raw) {

    // just forward to sub-path with empty path
    List<PathSegment> subPath = Collections.emptyList();
    return getDocumentPath(
        namespace, collection, id, subPath, where, fields, pageSize, pageState, profile, raw);
  }

  @Operation(
      summary = "Get a path in a document",
      description =
          """
          Retrieve the JSON representation of the document at a provided path.

          > Note that in case when conditions are given using the `where` query parameter, the response will contain an array of sub-documents where the condition is matched.
          The structure of returned sub-documents will only contain the path to the field which was included in the condition.
          Only single field conditions are possible at the moment. Multiple conditions targeting the same field are allowed.
          Mixing the `field` selection and `where` is not supported at the moment.
          The page size and page state parameters are only used together with `where` and enable paging through matched sub-documents.
          """)
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = OpenApiConstants.Parameters.NAMESPACE),
        @Parameter(name = "collection", ref = OpenApiConstants.Parameters.COLLECTION),
        @Parameter(name = "document-id", ref = OpenApiConstants.Parameters.DOCUMENT_ID),
        @Parameter(name = "document-path", ref = OpenApiConstants.Parameters.DOCUMENT_PATH),
        @Parameter(name = "where", ref = OpenApiConstants.Parameters.WHERE),
        @Parameter(name = "fields", ref = OpenApiConstants.Parameters.FIELDS),
        @Parameter(
            name = "page-size",
            in = ParameterIn.QUERY,
            description = "The max number of results to return.",
            schema = @Schema(implementation = Integer.class, defaultValue = "100", minimum = "1")),
        @Parameter(name = "page-state", ref = OpenApiConstants.Parameters.PAGE_STATE),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description =
            "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      implementation = DocumentResponseWrapper.class,
                      properties = @SchemaProperty(name = "data", type = SchemaType.DEFAULT)),
              examples = {
                @ExampleObject(ref = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE),
                @ExampleObject(ref = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE_UNWRAPPED),
                @ExampleObject(ref = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE_WITH_WHERE),
              })
        }),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
    @APIResponse(
        responseCode = "404",
        description = "Not found.",
        content =
            @Content(
                examples = {
                  @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                  @ExampleObject(ref = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST),
                  @ExampleObject(ref = OpenApiConstants.Examples.DOCUMENT_DOES_NOT_EXIST),
                },
                schema = @Schema(implementation = ApiError.class))),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
    @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
  })
  @GET
  @Path("{document-id}/{document-path:.*}")
  public Uni<RestResponse<Object>> getDocumentPath(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @PathParam("document-id") String id,
      @PathParam("document-path") List<PathSegment> path,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of results to return is one")
          Integer pageSize,
      @QueryParam("page-state") String pageState,
      @QueryParam("profile") boolean profile,
      @QueryParam("raw") boolean raw) {

    // fetch a valid table to ensure read is from a collection table
    return tableManager
        .getValidCollectionTable(namespace, collection)

        // if exists, then map to the read action
        .flatMap(
            t -> {
              boolean isSearch = where != null || pageSize != null;
              ExecutionContext context = ExecutionContext.create(profile);

              List<String> pathStrings = path.stream().map(PathSegment::getPath).toList();

              if (isSearch) {
                int pageSizeFinal = Optional.ofNullable(pageSize).orElse(100);
                Paginator paginator = new Paginator(pageState, pageSizeFinal);

                return readDocumentsService
                    .findSubDocuments(
                        namespace, collection, id, pathStrings, where, fields, paginator, context)
                    .map(
                        result -> {
                          if (null != result) {
                            return rawHandler(raw).apply(result);
                          } else {
                            return RestResponse.noContent();
                          }
                        });
              } else {
                return readDocumentsService
                    .getDocument(namespace, collection, id, pathStrings, fields, context)
                    .map(
                        result -> {
                          if (null != result) {
                            return rawHandler(raw).apply(result);
                          } else {
                            int code = Response.Status.NOT_FOUND.getStatusCode();
                            String msg = "A document with the id %s does not exist.".formatted(id);
                            ApiError error = new ApiError(msg, 404);
                            return RestResponse.ResponseBuilder.create(code).entity(error).build();
                          }
                        });
              }
            });
  }

  private Function<DocumentResponseWrapper<JsonNode>, RestResponse<Object>> rawHandler(
      boolean raw) {
    return wrapper -> {
      if (raw) {
        return RestResponse.ok(wrapper.data());
      } else {
        return RestResponse.ok(wrapper);
      }
    };
  }
}
