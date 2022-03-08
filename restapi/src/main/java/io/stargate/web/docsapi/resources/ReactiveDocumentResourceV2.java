package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Function;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.DocumentDBFactory;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.examples.WriteDocResponse;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.BuiltInApiFunction;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.MultiDocsResponse;
import io.stargate.web.docsapi.models.dto.ExecuteBuiltInFunction;
import io.stargate.web.docsapi.resources.async.AsyncObserver;
import io.stargate.web.docsapi.resources.error.ErrorHandler;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.ReactiveDocumentService;
import io.stargate.web.models.ApiError;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.Max;
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
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
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
public class ReactiveDocumentResourceV2 {

  private static final Logger logger = LoggerFactory.getLogger(ReactiveDocumentResourceV2.class);

  private static final String WHERE_DESCRIPTION =
      "a JSON blob with search filters;"
          + " allowed predicates: $eq, $ne, $in, $nin, $gt, $lt, $gte, $lte, $exists;"
          + " allowed boolean operators: $and, $or, $not;"
          + " allowed hints: $selectivity (a number between 0.0 and 1.0, less is better);"
          + " Use \\ to escape periods, commas, and asterisks.";

  @Inject private DocumentDBFactory dbFactory;
  @Inject private ReactiveDocumentService reactiveDocumentService;
  @Inject private DocsSchemaChecker schemaChecker;
  @Inject private ObjectMapper objectMapper;

  @POST
  @ManagedAsync
  @ApiOperation(
      value = "Create a new document",
      notes =
          "Auto-generates an ID for the newly created document. Use \\ to escape periods, commas, and asterisks.",
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 201,
            message = "Created",
            responseHeaders = @ResponseHeader(name = "Location"),
            response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id}")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces(MediaType.APPLICATION_JSON)
  public void postDoc(
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
      @ApiParam(value = "The JSON document", required = true)
          @NotBlank(message = "payload must not be empty")
          String payload,
      @ApiParam(
              value = "Include this to put a time-to-live on the document in the data store",
              required = false)
          @QueryParam("ttl")
          @Min(value = 1, message = "TTL value must be a positive integer.")
          Integer ttl,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false",
              required = false)
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // create table if needed, if not validate it's a doc table
    Single.fromCallable(
            () -> createOrValidateDbFromToken(authToken, request, namespace, collection))

        // then create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);

              // and call the document service to fire write
              return reactiveDocumentService
                  .writeDocument(db, namespace, collection, payload, ttl, context)

                  // map to response
                  .map(
                      result -> {
                        String url =
                            String.format(
                                "/v2/namespaces/%s/collections/%s/%s",
                                namespace, collection, result.getDocumentId());
                        String response = objectMapper.writeValueAsString(result);
                        return Response.created(URI.create(url)).entity(response).build();
                      });
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @POST
  @ManagedAsync
  @ApiOperation(
      value = "Write multiple documents in one request",
      notes =
          "Auto-generates an ID for the newly created document if an `idPath` is not provided as a query parameter. When an `idPath` is provided, this operation is idempotent. Note that this batch operation is not atomic and is not ordered. Any exception that happens during the write of one of the documents, does not influence the insertion of other documents. It's responsibility of the caller to examine the returned JSON and understand what documents were successfully written.",
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
  public void writeManyDocs(
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
          String payload,
      @ApiParam(
              value =
                  "The path where an ID could be found in each document. If defined, the value at this path will be used as the ID for each document. Otherwise, a random UUID will be given for each document.",
              required = false)
          @QueryParam("id-path")
          String idPath,
      @ApiParam(
              value = "Include this to put a time-to-live on the document in the data store",
              required = false)
          @QueryParam("ttl")
          @Min(value = 1, message = "TTL value must be a positive integer.")
          Integer ttl,
      @QueryParam("profile") Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // create table if needed, if not validate it's a doc table
    Single.fromCallable(
            () -> createOrValidateDbFromToken(authToken, request, namespace, collection))

        // then create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);

              // and call the document service to fire batch write
              return reactiveDocumentService
                  .writeDocuments(db, namespace, collection, payload, idPath, ttl, context)

                  // map to response
                  .map(
                      result -> {
                        String response = objectMapper.writeValueAsString(result);
                        return Response.accepted(response).build();
                      });
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @PUT
  @ManagedAsync
  @ApiOperation(value = "Create or update a document with the provided document-id")
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
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces(MediaType.APPLICATION_JSON)
  public void putDoc(
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
          @NotBlank(message = "payload must not be empty")
          String payload,
      @ApiParam(
              value = "Include this to put a time-to-live on the document in the data store",
              required = false)
          @QueryParam("ttl")
          @Min(value = 1, message = "TTL value must be a positive integer.")
          Integer ttl,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // create table if needed, if not validate it's a doc table
    Single.fromCallable(
            () -> createOrValidateDbFromToken(authToken, request, namespace, collection))

        // then generate id and create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);

              // and call the document service to fire write
              return reactiveDocumentService
                  .updateDocument(db, namespace, collection, id, payload, ttl, context)

                  // map to response
                  .map(
                      result -> {
                        String response = objectMapper.writeValueAsString(result);
                        return Response.ok().entity(response).build();
                      });
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @PUT
  @ManagedAsync
  @ApiOperation(
      value = "Replace data at a path in a document",
      notes =
          "Removes whatever was previously present at the path. Note that operation is not allowed if a JSON schema exist for a target collection.")
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
  public void putDocPath(
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
              value = "Include this to make the TTL match that of the parent document.",
              required = false)
          @QueryParam("ttl-auto")
          Boolean ttlAuto,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // create table if needed, if not validate it's a doc table
    Single.fromCallable(
            () -> createOrValidateDbFromToken(authToken, request, namespace, collection))

        // then generate id and create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<String> subPath =
                  path.stream().map(PathSegment::getPath).collect(Collectors.toList());

              // and call the document service to fire write
              return reactiveDocumentService
                  .updateSubDocument(
                      db, namespace, collection, id, subPath, payload, ttlAuto, context)

                  // map to response
                  .map(
                      result -> {
                        String response = objectMapper.writeValueAsString(result);
                        return Response.ok().entity(response).build();
                      });
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @PATCH
  @ManagedAsync
  @ApiOperation(
      value = "Update data at the root of a document",
      notes =
          "Merges data at the root with requested data. Note that operation is not allowed if a JSON schema exist for a target collection.")
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
  public void patchDoc(
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
              value = "Include this to match this data's TTL to the document's TTL",
              required = false)
          @QueryParam("ttl-auto")
          boolean ttlAuto,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // just delegate to patchPath with empty path
    List<PathSegment> path = Collections.emptyList();
    patchDocPath(
        authToken,
        namespace,
        collection,
        id,
        path,
        payload,
        ttlAuto,
        profile,
        request,
        asyncResponse);
  }

  @PATCH
  @ManagedAsync
  @ApiOperation(
      value = "Update data at a path in a document",
      notes =
          "Merges data at the path with requested data, assumes that the data at the path is already an object. Note that operation is not allowed if a JSON schema exist for a target collection.")
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
  public void patchDocPath(
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
              value = "Include this to match this data's TTL to the document's TTL",
              required = false)
          @QueryParam("ttl-auto")
          boolean ttlAuto,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // create table if needed, if not validate it's a doc table
    Single.fromCallable(
            () -> createOrValidateDbFromToken(authToken, request, namespace, collection))

        // then generate id and create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<String> subPath =
                  path.stream().map(PathSegment::getPath).collect(Collectors.toList());

              // and call the document service to fire write
              return reactiveDocumentService
                  .patchSubDocument(
                      db, namespace, collection, id, subPath, payload, ttlAuto, context)

                  // map to response
                  .map(
                      result -> {
                        String response = objectMapper.writeValueAsString(result);
                        return Response.ok().entity(response).build();
                      });
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Get a document",
      notes = "Retrieve the JSON representation of the document")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocumentResponseWrapper.class),
        @ApiResponse(code = 204, message = "No Content", response = ApiError.class),
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void getDocumentPath(
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
      @ApiParam(value = WHERE_DESCRIPTION) @QueryParam("where") String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(
              value = "the max number of results to return, if `where` is defined (default 100)",
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
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    getDocumentPath(
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
        request,
        asyncResponse);
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
        @ApiResponse(code = 204, message = "No Content", response = ApiError.class),
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}/{document-id}/{document-path: .*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void getDocumentPath(
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
                  "the path in the JSON that you want to retrieve. Use \\ to escape periods, commas, and asterisks.",
              required = true)
          @PathParam("document-path")
          List<PathSegment> path,
      @ApiParam(value = WHERE_DESCRIPTION) @QueryParam("where") String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(
              value = "the max number of results to return, if `where` is defined (default 100)",
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
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {

    // we do search if we have conditions, or the page size is defined and we need to page through
    // the results
    boolean isSearch = where != null || pageSizeParam != null;

    // init sequence
    Single.fromCallable(() -> getValidDbFromToken(authToken, request, namespace, collection))
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<String> pathStrings =
                  path.stream().map(PathSegment::getPath).collect(Collectors.toList());

              if (isSearch) {
                // execute search
                int pageSize = Optional.ofNullable(pageSizeParam).orElse(100);
                Paginator paginator = new Paginator(pageStateParam, pageSize);

                return reactiveDocumentService
                    .findSubDocuments(
                        db,
                        namespace,
                        collection,
                        id,
                        pathStrings,
                        where,
                        fields,
                        paginator,
                        context)
                    .map(rawDocumentHandler(raw))
                    .defaultIfEmpty(Response.noContent().build());
              } else {
                // execute get
                return reactiveDocumentService
                    .getDocument(db, namespace, collection, id, pathStrings, fields, context)
                    .map(rawDocumentHandler(raw))
                    .defaultIfEmpty(Response.status(Response.Status.NOT_FOUND).build());
              }
            })
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Search documents in a collection",
      notes = "Page over documents in a collection, with optional search parameters.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocumentResponseWrapper.class),
        @ApiResponse(code = 204, message = "No Content", response = ApiError.class),
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id: [a-zA-Z_0-9]+}")
  @Produces(MediaType.APPLICATION_JSON)
  public void searchDoc(
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
      @ApiParam(value = WHERE_DESCRIPTION) @QueryParam("where") String where,
      @ApiParam(
              value = "the field names that you want to restrict the results to",
              required = false)
          @QueryParam("fields")
          String fields,
      @ApiParam(
              value =
                  "the max number of documents to return, max "
                      + DocsApiConfiguration.MAX_PAGE_SIZE
                      + ", default 3",
              defaultValue = "3")
          @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of documents to return is one")
          @Max(
              value = 20,
              message =
                  "the max number of documents to return is " + DocsApiConfiguration.MAX_PAGE_SIZE)
          Integer pageSizeParam,
      @ApiParam(
              value = "Cassandra page state, used for pagination on consecutive requests",
              required = false)
          @QueryParam("page-state")
          String pageStateParam,
      // TODO: Someday, support this in a non-restrictive way
      // @QueryParam("sort") String sort,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    int pageSize = Optional.ofNullable(pageSizeParam).orElse(3);
    final Paginator paginator = new Paginator(pageStateParam, pageSize);
    // init sequence
    Single.fromCallable(() -> getValidDbFromToken(authToken, request, namespace, collection))
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              return reactiveDocumentService.findDocuments(
                  db, namespace, collection, where, fields, paginator, context);
            })
        .map(rawDocumentHandler(raw))
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  @POST
  @ManagedAsync
  @ApiOperation(
      value =
          "Execute a built-in function (e.g. $push and $pop) against a value in this document. Performance may vary.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = WriteDocResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("collections/{collection-id}/{document-id}/{document-path: .*}/function")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces(MediaType.APPLICATION_JSON)
  public void executeBuiltInFunction(
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
              value = "the path in the JSON that you want to execute the operation on",
              required = true)
          @PathParam("document-path")
          List<PathSegment> path,
      @ApiParam(value = "The operation to perform", required = true)
          @NotNull(message = "payload not provided")
          @Valid
          ExecuteBuiltInFunction payload,
      @ApiParam(
              value = "Whether to include profiling information in the response (advanced)",
              defaultValue = "false")
          @QueryParam("profile")
          Boolean profile,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw") Boolean raw,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    Single.fromCallable(
            () -> {
              DocumentDB db = getValidDbFromToken(authToken, request, namespace, collection);
              BuiltInApiFunction function = BuiltInApiFunction.fromName(payload.getOperation());
              if (function.requiresValue() && payload.getValue() == null) {
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Provided value must not be null");
              }
              return db;
            })
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<String> pathStrings =
                  path.stream().map(PathSegment::getPath).collect(Collectors.toList());

              return reactiveDocumentService
                  .executeBuiltInFunction(
                      db, namespace, collection, id, payload, pathStrings, context)
                  .map(rawDocumentHandler(raw))
                  .defaultIfEmpty(Response.status(Response.Status.NOT_FOUND).build());
            })
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
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
  public void deleteDoc(
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
      @ApiParam(value = "the id of the document", required = true) @PathParam("document-id")
          String id,
      @QueryParam("profile") Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    List<PathSegment> path = Collections.emptyList();
    deleteDocPath(authToken, namespace, collection, id, path, profile, request, asyncResponse);
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
  public void deleteDocPath(
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
      @ApiParam(value = "the id of the document", required = true) @PathParam("document-id")
          String id,
      @ApiParam(value = "the path in the JSON that you want to delete", required = true)
          @PathParam("document-path")
          List<PathSegment> path,
      @QueryParam("profile") Boolean profile,
      @Context HttpServletRequest request,
      @Suspended AsyncResponse asyncResponse) {
    // get valid db from token
    Single.fromCallable(() -> getValidDbFromToken(authToken, request, namespace, collection))

        // then create execution context
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              List<String> subPath =
                  path.stream().map(PathSegment::getPath).collect(Collectors.toList());

              // and call the document service to fire delete
              return reactiveDocumentService
                  .deleteDocument(db, namespace, collection, id, subPath, context)

                  // map to noContent response
                  .map(result -> Response.noContent().build());
            })

        // then subscribe
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }

  private DocumentDB getValidDbFromToken(
      String authToken, HttpServletRequest request, String namespace, String collection)
      throws UnauthorizedException {
    DocumentDB db = dbFactory.getDocDBForToken(authToken, getAllHeaders(request));
    schemaChecker.checkValidity(namespace, collection, db);
    return db;
  }

  private DocumentDB createOrValidateDbFromToken(
      String authToken, HttpServletRequest request, String namespace, String collection)
      throws UnauthorizedException {
    // get DB
    Map<String, String> headers = getAllHeaders(request);
    DocumentDB db = dbFactory.getDocDBForToken(authToken, headers);

    // try to create
    boolean created = db.maybeCreateTable(namespace, collection);

    // after creating the table, it can take up to 2 seconds for permissions cache to be updated,
    // but we can force the permissions re-fetch by logging in again.
    if (created) {
      // if created re-fetch
      db = dbFactory.getDocDBForToken(authToken, headers);
      db.maybeCreateTableIndexes(namespace, collection);
    } else {
      // if it was existing, simply validate
      schemaChecker.checkValidity(namespace, collection, db);
    }
    return db;
  }

  private Function<DocumentResponseWrapper<? extends JsonNode>, Response> rawDocumentHandler(
      Boolean raw) {
    return results -> {
      String result;
      if (raw != null && raw) {
        result = objectMapper.writeValueAsString(results.getData());
      } else {
        result = objectMapper.writeValueAsString(results);
      }
      return Response.ok(result).build();
    };
  }
}
