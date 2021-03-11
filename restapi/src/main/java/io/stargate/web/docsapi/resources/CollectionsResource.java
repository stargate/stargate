package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.models.DocCollection;
import io.stargate.web.docsapi.service.CollectionService;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"documents"})
@Path("/v2/namespaces/{namespace-id: [a-zA-Z_0-9]+}")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource {
  @Inject private Db db;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final CollectionService collectionService = new CollectionService();

  @GET
  @ApiOperation(value = "List collections in namespace")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("collections")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCollections(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "the namespace to fetch collections for", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);
          Set<Table> tables = (Set<Table>) authenticatedDB.getTables(namespace);

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(namespace),
                  tables.stream().map(SchemaEntity::name).collect(Collectors.toList()),
                  SourceAPI.REST);

          List<DocCollection> result =
              tables.stream()
                  .map(table -> collectionService.getCollectionInfo(table, db))
                  .collect(Collectors.toList());

          Object response = raw ? result : new ResponseWrapper(result);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @POST
  @ApiOperation(value = "Create a new empty collection in a namespace")
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created"),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("collections")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createCollection(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "the namespace to create the collection in", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(
              value = "JSON with the name of the collection",
              required = true,
              example = "{\"name\": \"example\"}")
          String payload,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          DocumentDB docDB = db.getDocDataStoreForToken(token, allHeaders);
          DocCollection info = mapper.readValue(payload, DocCollection.class);
          if (info.getName() == null) {
            throw new IllegalArgumentException("`name` is required to create a collection");
          }
          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  docDB.getAuthenticationSubject(),
                  namespace,
                  info.getName(),
                  Scope.CREATE,
                  SourceAPI.REST);

          boolean res = collectionService.createCollection(namespace, info.getName(), docDB);
          if (res) {
            return Response.status(Response.Status.CREATED).build();
          } else {
            return Response.status(Response.Status.CONFLICT)
                .entity(
                    String.format("Create failed: collection %s already exists", info.getName()))
                .build();
          }
        });
  }

  @DELETE
  @ApiOperation(value = "Delete a collection in a namespace")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("collections/{collection-id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteCollection(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "the namespace containing the collection to delete", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the collection to delete", required = true) @PathParam("collection-id")
          String collection,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  namespace,
                  collection,
                  Scope.DROP,
                  SourceAPI.REST);

          Table toDelete =
              authenticatedDB.getDataStore().schema().keyspace(namespace).table(collection);
          if (toDelete == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Bad request: Collection '%s' not found", collection),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }
          collectionService.deleteCollection(
              namespace,
              collection,
              new DocumentDB(
                  authenticatedDB.getDataStore(),
                  authenticatedDB.getAuthenticationSubject(),
                  db.getAuthorizationService()));
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @POST
  @ApiOperation(
      value = "Upgrade a collection in a namespace",
      notes =
          "WARNING: This endpoint is expected to cause some down-time for the collection you choose.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Collection not found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("collections/{collection-id}/upgrade")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response upgradeCollection(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "the namespace containing the collection to upgrade", required = true)
          @PathParam("namespace-id")
          String namespace,
      @ApiParam(value = "the collection to upgrade", required = true) @PathParam("collection-id")
          String collection,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(
              value = "JSON with the upgrade type",
              required = true,
              example = "{\"upgradeType\": \"SAI_INDEX_UPGRADE\"}")
          String payload,
      @Context HttpServletRequest servletRequest) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(servletRequest);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  namespace,
                  collection,
                  Scope.ALTER,
                  SourceAPI.REST);

          Table table = authenticatedDB.getTable(namespace, collection);
          if (table == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity("Collection not found")
                .build();
          }
          DocCollection request = mapper.readValue(payload, DocCollection.class);
          DocCollection info = collectionService.getCollectionInfo(table, db);
          if (request.getUpgradeType() == null
              || !info.getUpgradeAvailable()
              || info.getUpgradeType() != request.getUpgradeType()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity("That collection cannot be upgraded in that manner")
                .build();
          }

          boolean success =
              collectionService.upgradeCollection(
                  namespace,
                  collection,
                  new DocumentDB(
                      authenticatedDB.getDataStore(),
                      authenticatedDB.getAuthenticationSubject(),
                      db.getAuthorizationService()),
                  request.getUpgradeType());

          if (success) {
            table = authenticatedDB.getTable(namespace, collection);
            info = collectionService.getCollectionInfo(table, db);

            Object response = raw ? info : new ResponseWrapper(info);
            return Response.status(Response.Status.OK)
                .entity(Converters.writeResponse(response))
                .build();
          } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Collection was not upgraded.")
                .build();
          }
        });
  }
}
