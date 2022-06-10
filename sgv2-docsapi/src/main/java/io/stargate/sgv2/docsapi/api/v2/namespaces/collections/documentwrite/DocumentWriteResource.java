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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documentwrite;

import com.fasterxml.jackson.databind.JsonNode;
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
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonDocumentShredder;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import io.stargate.sgv2.docsapi.service.write.DocumentWriteService;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
@Path(DocumentWriteResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.DOCUMENTS)
public class DocumentWriteResource {

  private static final Logger LOG = LoggerFactory.getLogger(DocumentWriteResource.class);

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace:\\w+}/collections/{collection:\\w}";
  public static final String BASE_PATH_WITH_ID = BASE_PATH + "/{document-id:\\w+}";

  @Inject @Authorized TableManager tableManager;

  @Inject DocumentWriteService documentWriteService;

  @Inject DataStoreProperties dataStoreProperties;

  @Inject JsonDocumentShredder documentShredder;

  @Operation(
      description = "Create a new document. If the collection does not exist, it will be created.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace to write the document to."),
        @Parameter(
            name = "collection",
            ref = OpenApiConstants.Parameters.COLLECTION,
            description = "The collection to write the document to."),
        @Parameter(name = "profile", ref = OpenApiConstants.Parameters.PROFILE),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "201",
            description = "Document created. The ID will be returned.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = SimpleResponseWrapper.class,
                          properties =
                              @SchemaProperty(name = "documentId", type = SchemaType.STRING)))
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
  public Uni<RestResponse<Object>> createDocument(
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection,
      @QueryParam("profile") boolean profile,
      @NotNull JsonNode body) {

    // get all valid collection tables
    return tableManager
        .ensureValidDocumentTable(namespace, collection)
        .onItem()
        .transform(
            __ -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<JsonShreddedRow> rows = documentShredder.shred(body, Collections.emptyList());
              documentWriteService.writeDocument(
                  namespace, collection, UUID.randomUUID().toString(), rows, null, profile);
            });
  }
}
