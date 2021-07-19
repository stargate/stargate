package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.examples.WriteDocResponse;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.MultiDocsResponse;
import io.stargate.web.docsapi.resources.error.ErrorHandler;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.models.Error;
import io.stargate.web.resources.Db;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
public class DocumentResourceV2 {
  @Inject private Db dbFactory;
  private static final Logger logger = LoggerFactory.getLogger(DocumentResourceV2.class);
  @Inject private ObjectMapper mapper;
  @Inject private DocumentService documentService;
  @Inject private DocsApiConfiguration docsApiConfiguration;
  @Inject private DocsSchemaChecker schemaChecker;

  public DocumentResourceV2() {
    // default constructor for injection-based call paths
  }

  @VisibleForTesting
  public DocumentResourceV2(
      Db dbFactory,
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
      value = "Create a new document",
      notes = "Auto-generates an ID for the newly created document",
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 201,
            message = "Created",
            responseHeaders = @ResponseHeader(name = "Location"),
            response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id}")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public Response postDoc(
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
      @ApiParam(value = "The JSON document", required = true) String payload,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false",
              required = false)
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request) {
    // This route does nearly the same thing as PUT, except that it assigns an ID for the requester
    // And returns it as a Location header/in JSON body
    logger.debug("Post: Collection = {}", collection);
    String newId = UUID.randomUUID().toString();
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
              newId,
              payload,
              new ArrayList<>(),
              false,
              dbFactory,
              isJson,
              getAllHeaders(request),
              context);

          return Response.created(
                  URI.create(
                      String.format(
                          "/v2/namespaces/%s/collections/%s/%s", namespace, collection, newId)))
              .entity(
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(newId, null, null, context.toProfile())))
              .build();
        });
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
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
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

  @PUT
  @ManagedAsync
  @ApiOperation(value = "Create or update a document with the provided document-id")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id}/{document-id}")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public Response putDoc(
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
    logger.debug("Put: Collection = {}, id = {}", collection, id);
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
              false,
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

  @PUT
  @ManagedAsync
  @ApiOperation(
      value = "Replace data at a path in a document",
      notes = "Removes whatever was previously present at the path")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id}/{document-id}/{document-path: .*}")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public Response putDocPath(
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
    logger.debug("Put: Collection = {}, id = {}, path = {}", collection, id, path);
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
              false,
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
      value = "Update data at the root of a document",
      notes = "Merges data at the root with requested data.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
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
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
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
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
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
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, allHeaders);
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
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
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
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, allHeaders);
          documentService.deleteAtPath(db, namespace, collection, id, path);
          return Response.noContent().build();
        });
  }

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Get a document",
      notes = "Retrieve the JSON representation of the document")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocumentResponseWrapper.class),
        @ApiResponse(code = 204, message = "No Content", response = Error.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDoc(
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
      @ApiParam(
              value =
                  "a JSON blob with search filters, allowed operators: $eq, $ne, $in, $nin, $gt, $lt, $gte, $lte, $exists",
              required = false)
          @QueryParam("where")
          String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(
              value = "the max number of results to return, if `where` is defined.",
              defaultValue = "100")
          @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of results to return is one")
          Integer pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request) {
    return getDocPath(
        headers,
        ui,
        authToken,
        namespace,
        collection,
        id,
        new ArrayList<>(),
        where,
        fields,
        pageSizeParam,
        pageStateParam,
        profile,
        raw,
        request);
  }

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Get a path in a document",
      notes =
          "Retrieve the JSON representation of the document at a provided path, with optional search parameters.",
      response = DocumentResponseWrapper.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocumentResponseWrapper.class),
        @ApiResponse(code = 204, message = "No Content", response = Error.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}/{document-path: .*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDocPath(
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
      @ApiParam(
              value =
                  "a JSON blob with search filters, allowed operators: $eq, $ne, $in, $nin, $gt, $lt, $gte, $lte, $exists",
              required = false)
          @QueryParam("where")
          String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(
              value = "the max number of results to return, if `where` is defined",
              defaultValue = "100")
          @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of results to return is one")
          Integer pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request) {
    return handle(
        () -> {
          int pageSize = Optional.ofNullable(pageSizeParam).orElse(100);
          Map<String, String> allHeaders = getAllHeaders(request);
          List<FilterCondition> filters = new ArrayList<>();
          List<String> selectionList = new ArrayList<>();

          if (fields != null) {
            try {
              JsonNode fieldsJson = mapper.readTree(fields);
              selectionList = documentService.convertToSelectionList(fieldsJson);
            } catch (JsonProcessingException e) {
              throw new ErrorCodeRuntimeException(
                  ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, "Malformed fields array provided.");
            }
          }

          if (where != null) {
            try {
              JsonNode filterJson = mapper.readTree(where);
              filters = documentService.convertToFilterOps(path, filterJson);
            } catch (JsonProcessingException e) {
              throw new ErrorCodeRuntimeException(
                  ErrorCode.DOCS_API_SEARCH_WHERE_JSON_INVALID,
                  "Malformed search conditions provided.");
            }
          }

          if (!filters.isEmpty()) {
            Set<String> distinctFields =
                filters.stream().map(FilterCondition::getFullFieldPath).collect(Collectors.toSet());
            if (distinctFields.size() > 1) {
              String msg =
                  String.format(
                      "Conditions across multiple fields are not yet supported. Found: %s.",
                      distinctFields);
              throw new ErrorCodeRuntimeException(
                  ErrorCode.DOCS_API_GET_MULTIPLE_FIELD_CONDITIONS, msg);
            }
            String fieldName = filters.get(0).getField();
            if (!selectionList.isEmpty() && !selectionList.contains(fieldName)) {
              throw new ErrorCodeRuntimeException(
                  ErrorCode.DOCS_API_GET_CONDITION_FIELDS_NOT_REFERENCED);
            }
          }

          // check first that namespace and table exist
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, allHeaders);
          schemaChecker.checkValidity(namespace, collection, db);

          ExecutionContext context = ExecutionContext.create(profile);

          JsonNode node;
          // Fetch the whole doc at the specified path only if the request does not have an explicit
          // page size. Otherwise, produce paginated results for child data at the specified path.
          if (filters.isEmpty() && pageSizeParam == null) {
            node =
                documentService.getJsonAtPath(
                    db, namespace, collection, id, path, selectionList, context);
            if (node == null) {
              return Response.status(Response.Status.NOT_FOUND).build();
            }

            String json;
            if (raw == null || !raw) {
              json =
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(id, null, node, context.toProfile()));
            } else {
              json = mapper.writeValueAsString(node);
            }

            logger.debug(json);
            return Response.ok(json).build();
          } else {
            final Paginator paginator = new Paginator(pageStateParam, pageSize);
            JsonNode result =
                documentService.searchDocumentsV2(
                    db,
                    namespace,
                    collection,
                    path,
                    filters,
                    selectionList,
                    id,
                    paginator,
                    context);

            if (result == null) {
              return Response.noContent().build();
            }

            String json;

            if (raw == null || !raw) {
              String pagingStateStr = paginator.makeExternalPagingState();
              json =
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(
                          id, pagingStateStr, result, context.toProfile()));
            } else {
              json = mapper.writeValueAsString(result);
            }

            logger.debug(json);
            return Response.ok(json).build();
          }
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
