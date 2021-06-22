package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import io.reactivex.rxjava3.core.Single;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.resources.async.AsyncObserver;
import io.stargate.web.docsapi.resources.error.ErrorHandler;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.ReactiveDocumentService;
import io.stargate.web.models.Error;
import io.stargate.web.resources.Db;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ManagedAsync;

@Path("/v2/namespaces/{namespace-id: [a-zA-Z_0-9]+}")
@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"documents"})
@Produces(MediaType.APPLICATION_JSON)
public class ReactiveDocumentResourceV2 {

  @Inject private Db dbFactory;
  @Inject private ReactiveDocumentService reactiveDocumentService;
  @Inject private DocsSchemaChecker schemaChecker;

  @GET
  @ManagedAsync
  @ApiOperation(
      value = "Search documents in a collection",
      notes =
          "Page over documents in a collection, with optional search parameters. Does not perform well for large documents.")
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
  public void searchDoc(
      @Context HttpHeaders headers,
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
      @ApiParam(
              value = "the max number of documents to return, max " + DocumentDB.MAX_PAGE_SIZE,
              defaultValue = "1")
          @QueryParam("page-size")
          @Min(value = 1, message = "the minimum number of documents to return is one")
          @Max(
              value = DocumentDB.MAX_PAGE_SIZE,
              message = "the max number of documents to return is " + DocumentDB.MAX_PAGE_SIZE)
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
    int pageSize = Optional.ofNullable(pageSizeParam).orElse(1);
    final Paginator paginator = new Paginator(pageStateParam, pageSize);
    // init sequence
    Single.fromCallable(
            () -> {
              DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, getAllHeaders(request));
              schemaChecker.checkValidity(namespace, collection, db);
              return db;
            })
        .flatMap(
            db -> {
              ExecutionContext context = ExecutionContext.create(profile);
              return reactiveDocumentService.findDocuments(
                  db, namespace, collection, where, fields, paginator, context);
            })
        .map(
            results -> {
              if (raw == null || !raw) {
                return Response.ok(results).build();
              } else {
                return Response.ok(results.getData()).build();
              }
            })
        .safeSubscribe(
            AsyncObserver.forResponseWithHandler(
                asyncResponse, ErrorHandler.EXCEPTION_TO_RESPONSE));
  }
}
