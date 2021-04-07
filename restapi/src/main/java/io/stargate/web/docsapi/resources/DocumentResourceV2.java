package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.examples.WriteDocResponse;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.exception.ResourceNotFoundException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.models.Error;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Db;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
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
  private static final ObjectMapper mapper = new ObjectMapper();
  private final DocumentService documentService;
  private final int DEFAULT_PAGE_SIZE = DocumentDB.SEARCH_PAGE_SIZE;

  public DocumentResourceV2() {
    documentService = new DocumentService();
  }

  @VisibleForTesting
  DocumentResourceV2(Db dbFactory, DocumentService documentService) {
    this.dbFactory = dbFactory;
    this.documentService = documentService;
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
              getAllHeaders(request));

          return Response.created(
                  URI.create(
                      String.format(
                          "/v2/namespaces/%s/collections/%s/%s", namespace, collection, newId)))
              .entity(mapper.writeValueAsString(new DocumentResponseWrapper<>(newId, null, null)))
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
      @ApiParam(value = "The JSON document", required = true) String payload,
      @Context HttpServletRequest request) {
    logger.debug("Put: Collection = {}, id = {}", collection, id);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");
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
              getAllHeaders(request));
          return Response.ok()
              .entity(mapper.writeValueAsString(new DocumentResponseWrapper<>(id, null, null)))
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
      @ApiParam(value = "The JSON document", required = true) String payload,
      @Context HttpServletRequest request) {
    logger.debug("Put: Collection = {}, id = {}, path = {}", collection, id, path);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");
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
              getAllHeaders(request));
          return Response.ok()
              .entity(mapper.writeValueAsString(new DocumentResponseWrapper<>(id, null, null)))
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
      @ApiParam(value = "The JSON document", required = true) String payload,
      @Context HttpServletRequest request) {
    logger.debug("Patch: Collection = {}, id = {}", collection, id);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");
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
              getAllHeaders(request));
          return Response.ok()
              .entity(mapper.writeValueAsString(new DocumentResponseWrapper<>(id, null, null)))
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
      @ApiParam(value = "The JSON document", required = true) String payload,
      @Context HttpServletRequest request) {
    logger.debug("Patch: Collection = {}, id = {}, path = {}", collection, id, path);
    return handle(
        () -> {
          boolean isJson =
              headers
                  .getHeaderString(HttpHeaders.CONTENT_TYPE)
                  .toLowerCase()
                  .contains("application/json");
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
              getAllHeaders(request));
          return Response.ok()
              .entity(mapper.writeValueAsString(new DocumentResponseWrapper<>(id, null, null)))
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
          int pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
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
          int pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request) {
    return handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          List<FilterCondition> filters = new ArrayList<>();
          List<String> selectionList = new ArrayList<>();

          if (where != null) {
            JsonNode filterJson = mapper.readTree(where);
            filters = documentService.convertToFilterOps(path, filterJson);
            if (fields != null) {
              JsonNode fieldsJson = mapper.readTree(fields);
              selectionList = documentService.convertToSelectionList(fieldsJson);
            }
          } else if (fields != null) {
            throw new DocumentAPIRequestException(
                "Selecting fields is not allowed without `where`");
          }

          if (!filters.isEmpty()) {
            Set<String> distinctFields =
                filters.stream().map(FilterCondition::getFullFieldPath).collect(Collectors.toSet());
            if (distinctFields.size() > 1) {
              throw new DocumentAPIRequestException(
                  String.format(
                      "Conditions across multiple fields are not yet supported (found: %s)",
                      distinctFields));
            }
            String fieldName = filters.get(0).getField();
            if (!selectionList.isEmpty() && !selectionList.contains(fieldName)) {
              throw new DocumentAPIRequestException(
                  "When selecting `fields`, the field referenced by `where` must be in the selection.");
            }
          }

          // check first that namespace and table exist
          AuthenticatedDB authenticatedDB = dbFactory.getDataStoreForToken(authToken, allHeaders);
          Keyspace keyspace = authenticatedDB.getKeyspace(namespace);
          if (null == keyspace) {
            throw new ResourceNotFoundException(
                String.format("Namespace %s does not exist.", namespace));
          }
          Table table = keyspace.table(collection);
          if (null == table) {
            throw new ResourceNotFoundException(
                String.format("Collection %s does not exist.", collection));
          }

          JsonNode node;
          if (filters.isEmpty()) {

            DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, allHeaders);
            node = documentService.getJsonAtPath(db, namespace, collection, id, path);
            if (node == null) {
              return Response.noContent().build();
            }

            String json;
            if (raw == null || !raw) {
              json = mapper.writeValueAsString(new DocumentResponseWrapper<>(id, null, node));
            } else {
              json = mapper.writeValueAsString(node);
            }

            logger.debug(json);
            return Response.ok(json).build();
          } else {
            final Paginator paginator =
                new Paginator(
                    pageStateParam,
                    pageSizeParam,
                    pageSizeParam > 0 ? pageSizeParam : DEFAULT_PAGE_SIZE);

            DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, getAllHeaders(request));
            JsonNode result =
                documentService.searchDocumentsV2(
                    db, namespace, collection, filters, selectionList, id, paginator);

            if (result == null) {
              return Response.noContent().build();
            }

            String json;

            if (raw == null || !raw) {
              String pagingStateStr = paginator.getDocumentPageStateAsString();
              json =
                  mapper.writeValueAsString(
                      new DocumentResponseWrapper<>(id, pagingStateStr, result));
            } else {
              json = mapper.writeValueAsString(result);
            }

            logger.debug(json);
            return Response.ok(json).build();
          }
        });
  }

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Search documents in a collection",
      notes =
          "Page over documents in a collection, with optional search parameters. Does not perform well for large documents.",
      response = DocumentResponseWrapper.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocumentResponseWrapper.class),
        @ApiResponse(code = 204, message = "No Content", response = Error.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchDoc(
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
      @ApiParam(
              value =
                  "a JSON blob with search filters, allowed operators: $eq, $ne, $in, $nin, $gt, $lt, $gte, $lte, $exists")
          @QueryParam("where")
          String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(value = "the max number of documents to return, max 20", defaultValue = "1")
          @QueryParam("page-size")
          int pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
      // TODO: Someday, support this in a non-restrictive way
      // @QueryParam("sort") String sort,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request) {
    return handle(
        () -> {
          List<FilterCondition> filters = new ArrayList<>();
          List<String> selectionList = new ArrayList<>();
          if (where != null) {
            JsonNode filterJson = mapper.readTree(where);
            filters = documentService.convertToFilterOps(new ArrayList<>(), filterJson);
          }

          if (fields != null) {
            JsonNode fieldsJson = mapper.readTree(fields);
            selectionList = documentService.convertToSelectionList(fieldsJson);
          }

          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, getAllHeaders(request));

          final Paginator paginator =
              new Paginator(pageStateParam, pageSizeParam, DEFAULT_PAGE_SIZE);

          JsonNode results;
          if (filters.isEmpty()) {
            results =
                documentService.getFullDocuments(
                    db, namespace, collection, selectionList, paginator);
          } else {
            results =
                documentService.getFullDocumentsFiltered(
                    db, namespace, collection, filters, selectionList, paginator);
          }

          if (results == null) {
            return Response.noContent().build();
          }

          String json;
          if (raw == null || !raw) {
            json =
                mapper.writeValueAsString(
                    new DocumentResponseWrapper<>(
                        null, paginator.getDocumentPageStateAsString(), results));
          } else {
            json = mapper.writeValueAsString(results);
          }

          logger.debug(json);
          return Response.ok(json).build();
        });
  }

  static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (UnauthorizedException ue) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(
              new Error(
                  "Role unauthorized for operation: " + ue.getMessage(),
                  Response.Status.UNAUTHORIZED.getStatusCode()))
          .build();
    } catch (DocumentAPIRequestException sre) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new Error(
                  "Bad request: " + sre.getLocalizedMessage(),
                  Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (ResourceNotFoundException nfe) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(
              new Error(
                  "Not found: " + nfe.getLocalizedMessage(),
                  Response.Status.NOT_FOUND.getStatusCode()))
          .build();
    } catch (NoNodeAvailableException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(
              new Error(
                  "Internal connection to Cassandra closed",
                  Response.Status.SERVICE_UNAVAILABLE.getStatusCode()))
          .build();
    } catch (Throwable t) {
      logger.error("Error when executing request", t);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new Error(
                  "Server error: " + t.getLocalizedMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
  }
}
