package io.stargate.web.docsapi.resources;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.models.DocCollection;
import io.stargate.web.docsapi.models.dto.CreateCollection;
import io.stargate.web.docsapi.models.dto.UpgradeCollection;
import io.stargate.web.docsapi.service.CollectionService;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
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
  @Inject private CollectionService collectionService;
  @Inject private DocsSchemaChecker schemaChecker;

  @GET
  @ApiOperation(
      value = "List collections in namespace",
      response = DocCollection.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocCollection.class),
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
          DocumentDB docDB = db.getDocDataStoreForToken(token, getAllHeaders(request));
          Keyspace ks = docDB.schema().keyspace(namespace);
          if (ks == null) {
            throw new NotFoundException(String.format("keyspace '%s' not found", namespace));
          }
          Collection<Table> tables = ks.tables();

          tables =
              tables.stream()
                  .filter(table -> schemaChecker.isValid(namespace, table.name(), docDB))
                  .collect(Collectors.toList());

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  docDB.getAuthenticationSubject(),
                  Collections.singletonList(namespace),
                  tables.stream().map(SchemaEntity::name).collect(Collectors.toList()),
                  SourceAPI.REST,
                  ResourceKind.TABLE);

          List<DocCollection> result =
              tables.stream()
                  .map(table -> collectionService.getCollectionInfo(table, db))
                  .collect(Collectors.toList());

          Object response = raw ? result : new ResponseWrapper<>(result);
          return Response.status(Response.Status.OK).entity(response).build();
        });
  }

  @POST
  @ApiOperation(value = "Create a new empty collection in a namespace")
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
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
          @NotNull(message = "payload not provided")
          @Valid
          CreateCollection body,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          DocumentDB docDB = db.getDocDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  docDB.getAuthenticationSubject(),
                  namespace,
                  body.getName(),
                  Scope.CREATE,
                  SourceAPI.REST,
                  ResourceKind.TABLE);

          boolean res = collectionService.createCollection(namespace, body.getName(), docDB);
          if (res) {
            return Response.status(Response.Status.CREATED).build();
          } else {
            return Response.status(Response.Status.CONFLICT)
                .entity(
                    new Error(
                        String.format(
                            "Create failed: collection %s already exists.", body.getName()),
                        Response.Status.CONFLICT.getStatusCode()))
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
          DocumentDB docDB = db.getDocDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  docDB.getAuthenticationSubject(),
                  namespace,
                  collection,
                  Scope.DROP,
                  SourceAPI.REST,
                  ResourceKind.TABLE);

          Table toDelete = docDB.schema().keyspace(namespace).table(collection);
          if (toDelete == null || !schemaChecker.isValid(namespace, collection, docDB)) {
            String msg = String.format("Collection '%s' not found.", collection);
            return ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST.toResponse(msg);
          }

          collectionService.deleteCollection(namespace, collection, docDB);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @POST
  @ApiOperation(
      value = "Upgrade a collection in a namespace",
      response = DocCollection.class,
      notes =
          "WARNING: This endpoint is expected to cause some down-time for the collection you choose.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = DocCollection.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Collection not found", response = Error.class),
        @ApiResponse(code = 422, message = "Unprocessable entity", response = Error.class),
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
          @NotNull(message = "payload not provided")
          @Valid
          UpgradeCollection body,
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
                  SourceAPI.REST,
                  ResourceKind.TABLE);

          Table table = authenticatedDB.getTable(namespace, collection);
          if (table == null) {
            String msg = String.format("Collection %s not found.", collection);
            return ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST.toResponse(msg);
          }

          DocCollection info = collectionService.getCollectionInfo(table, db);
          if (!info.getUpgradeAvailable()
              || !Objects.equals(info.getUpgradeType(), body.getUpgradeType())) {
            return ErrorCode.DOCS_API_GENERAL_UPGRADE_INVALID.toResponse();
          }

          boolean success =
              collectionService.upgradeCollection(
                  namespace,
                  collection,
                  new DocumentDB(
                      authenticatedDB.getDataStore(),
                      authenticatedDB.getAuthenticationSubject(),
                      db.getAuthorizationService()),
                  body.getUpgradeType());

          if (success) {
            table = authenticatedDB.getTable(namespace, collection);
            info = collectionService.getCollectionInfo(table, db);

            Object response = raw ? info : new ResponseWrapper<>(info);
            return Response.status(Response.Status.OK).entity(response).build();
          } else {
            Error error =
                new Error(
                    "Collection was not upgraded.",
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(error).build();
          }
        });
  }
}
