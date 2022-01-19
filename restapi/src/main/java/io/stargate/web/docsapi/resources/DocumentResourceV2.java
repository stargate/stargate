package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.DocumentDBFactory;
import io.stargate.web.docsapi.examples.WriteDocResponse;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.MultiDocsResponse;
import io.stargate.web.docsapi.resources.error.ErrorHandler;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.models.ApiError;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v2/namespaces/{namespace-id: [a-zA-Z_0-9]+}")
@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"documents"})
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class DocumentResourceV2 {
  private static final Logger logger = LoggerFactory.getLogger(DocumentResourceV2.class);

  @Inject private DocumentDBFactory dbFactory;
  @Inject private ObjectMapper mapper;
  @Inject private DocumentService documentService;
  @Inject private DocsApiConfiguration docsApiConfiguration;
  @Inject private DocsSchemaChecker schemaChecker;

  public DocumentResourceV2() {
    // default constructor for injection-based call paths
  }

  @VisibleForTesting
  public DocumentResourceV2(
      DocumentDBFactory dbFactory,
      ObjectMapper mapper,
      DocumentService documentService,
      DocsApiConfiguration docsApiConfiguration,
      DocsSchemaChecker schemaChecker) {
    this.dbFactory = dbFactory;
    this.mapper = mapper;
    this.documentService = documentService;
    this.docsApiConfiguration = docsApiConfiguration;
    this.schemaChecker = schemaChecker;
  }

  @POST
  @ManagedAsync
  @ApiOperation(
      value = "Write multiple documents in one request",
      notes =
          "Auto-generates an ID for the newly created document if an idPath is not provided as a query parameter. When an idPath is provided, this operation is idempotent.",
      code = 202)
  @ApiResponses(
      value = {
        @ApiResponse(code = 202, message = "Accepted", response = MultiDocsResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id}/batch")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response writeManyDocs(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String authToken,
      @ApiParam(value = "the namespace that the collection is in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the name of the collection", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "A JSON array where each element is a document to write", required = true)
          @NonNull
          InputStream payload,
      @ApiParam(
              value =
                  "The path where an ID could be found in each document. If defined, the value at this path will be used as the ID for each document. Otherwise, a random UUID will be given for each document.",
              required = false)
          @QueryParam("id-path")
          String idPath,
      @QueryParam("profile") Boolean profile,
      @Context HttpServletRequest request) {
    // This route does nearly the same thing as PUT, except that it assigns an ID for the requester
    // And returns it as a Location header/in JSON body
    logger.debug("Batch Write: Collection = {}", collection);
    return handle(
        () -> {
          ExecutionContext context = ExecutionContext.create(profile);
          List<String> idsCreated =
              documentService.writeManyDocs(
                  authToken,
                  namespace,
                  collection,
                  payload,
                  Optional.ofNullable(idPath),
                  dbFactory,
                  context,
                  getAllHeaders(request));

          return Response.accepted()
              .entity(
                  mapper.writeValueAsString(new MultiDocsResponse(idsCreated, context.toProfile())))
              .build();
        });
  }

  @PATCH
  @ManagedAsync
  @ApiOperation(
      value = "Update data at the root of a document",
      notes = "Merges data at the root with requested data.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id}/{document-id}")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public Response patchDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String authToken,
      @ApiParam(value = "the namespace that the collection is in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the name of the collection", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "the name of the document", required = true) @PathParam("document-id")
          String id,
      @ApiParam(value = "The JSON document", required = true)
          @NotNull(message = "payload not provided")
          @NotBlank(message = "payload must not be empty")
          String payload,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request) {
    logger.debug("Patch: Collection = {}, id = {}", collection, id);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");

          ExecutionContext context = ExecutionContext.create(profile);

          documentService.putAtPath(
              authToken,
              namespace,
              collection,
              id,
              payload,
              new ArrayList<>(),
              true,
              dbFactory,
              isJson,
              getAllHeaders(request),
              context);
          return Response.ok()
              .entity(
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(id, null, null, context.toProfile())))
              .build();
        });
  }

  @PATCH
  @ManagedAsync
  @ApiOperation(
      value = "Update data at a path in a document",
      notes =
          "Merges data at the path with requested data, assumes that the data at the path is already an object.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id}/{document-id}/{document-path: .*}")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public Response patchDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String authToken,
      @ApiParam(value = "the namespace that the collection is in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the name of the collection", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "the name of the document", required = true) @PathParam("document-id")
          String id,
      @ApiParam(value = "the path in the JSON that you want to retrieve", required = true)
          @PathParam("document-path")
          List<PathSegment> path,
      @ApiParam(value = "The JSON document", required = true)
          @NotNull(message = "payload not provided")
          @NotBlank(message = "payload must not be empty")
          String payload,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request) {
    logger.debug("Patch: Collection = {}, id = {}, path = {}", collection, id, path);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");

          ExecutionContext context = ExecutionContext.create(profile);

          documentService.putAtPath(
              authToken,
              namespace,
              collection,
              id,
              payload,
              path,
              true,
              dbFactory,
              isJson,
              getAllHeaders(request),
              context);
          return Response.ok()
              .entity(
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(id, null, null, context.toProfile())))
              .build();
        });
  }

  @DELETE
  @ManagedAsync
  @ApiOperation(value = "Delete a document", notes = "Delete a document")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String authToken,
      @ApiParam(value = "the namespace that the collection is in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the name of the collection", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "the name of the document", required = true) @PathParam("document-id")
          String id,
      @Context HttpServletRequest request) {
    logger.debug("Delete: Collection = {}, id = {}, path = {}", collection, id, new ArrayList<>());
    return handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          DocumentDB db = dbFactory.getDocDBForToken(authToken, allHeaders);
          documentService.deleteAtPath(db, namespace, collection, id, new ArrayList<>());
          return Response.noContent().build();
        });
  }

  @DELETE
  @ManagedAsync
  @ApiOperation(value = "Delete a path in a document", notes = "Delete a path in a document")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}/{document-path: .*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String authToken,
      @ApiParam(value = "the namespace that the collection is in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the name of the collection", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "the name of the document", required = true) @PathParam("document-id")
          String id,
      @ApiParam(value = "the path in the JSON that you want to retrieve", required = true)
          @PathParam("document-path")
          List<PathSegment> path,
      @Context HttpServletRequest request) {
    logger.debug("Delete: Collection = {}, id = {}, path = {}", collection, id, path);
    return handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          DocumentDB db = dbFactory.getDocDBForToken(authToken, allHeaders);
          documentService.deleteAtPath(db, namespace, collection, id, path);
          return Response.noContent().build();
        });
  }

  static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (Throwable t) {
      return ErrorHandler.EXCEPTION_TO_RESPONSE.apply(t);
    }
  }
}
