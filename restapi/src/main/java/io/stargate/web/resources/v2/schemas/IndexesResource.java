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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.resources.v2.schemas;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.codahale.metrics.annotation.Timed;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.models.Error;
import io.stargate.web.models.IndexAdd;
import io.stargate.web.models.SuccessResponse;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/keyspaces/{keyspaceName}/indexes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IndexesResource {
  @Inject private Db db;

  @Timed
  @POST
  @ApiOperation(
      value = "Add an index to a table's column",
      notes = "Add an index to a single column of a table.",
      response = SuccessResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  public Response addIndex(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(required = true) @NotNull final IndexAdd indexAdd,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          String tableName = indexAdd.getTable();
          String columnName = indexAdd.getColumn();

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          try {
            final Table tableMetadata = authenticatedDB.getTable(keyspaceName, tableName);
            final Column col = tableMetadata.column(columnName);
            if (col == null) {
              return Response.status(Response.Status.NOT_FOUND)
                  .entity(
                      new Error(
                          String.format("Column '%s' not found in table.", columnName),
                          Response.Status.NOT_FOUND.getStatusCode()))
                  .build();
            }
          } catch (NotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Table '%s' not found in keyspace.", tableName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          createIndex(keyspaceName, indexAdd, authenticatedDB, tableName, columnName);

          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  private void createIndex(
      String keyspaceName,
      IndexAdd indexAdd,
      AuthenticatedDB authenticatedDB,
      String tableName,
      String columnName)
      throws UnauthorizedException, InterruptedException, java.util.concurrent.ExecutionException {
    db.getAuthorizationService()
        .authorizeSchemaWrite(
            authenticatedDB.getAuthenticationSubject(),
            keyspaceName,
            tableName,
            Scope.CREATE,
            SourceAPI.REST);

    boolean indexKeys = false;
    boolean indexEntries = false;
    boolean indexValues = false;
    boolean indexFull = false;
    if (indexAdd.getKind() != null) {
      switch (indexAdd.getKind()) {
        case KEYS:
          indexKeys = true;
          break;
        case VALUES:
          indexValues = true;
          break;
        case ENTRIES:
          indexEntries = true;
          break;
        case FULL:
          indexFull = true;
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Invalid indexKind value: %s", indexAdd.getKind()));
      }
    }

    CollectionIndexingType indexingType =
        ImmutableCollectionIndexingType.builder()
            .indexEntries(indexEntries)
            .indexKeys(indexKeys)
            .indexValues(indexValues)
            .indexFull(indexFull)
            .build();

    authenticatedDB
        .getDataStore()
        .queryBuilder()
        .create()
        .custom(indexAdd.getType())
        .index(indexAdd.getName())
        .ifNotExists(indexAdd.getIfNotExists())
        .on(keyspaceName, tableName)
        .column(columnName)
        .indexingType(indexingType)
        .build()
        .execute()
        .get();
  }

  @Timed
  @DELETE
  @ApiOperation(
      value = "Drop an index from keyspace",
      notes = "Drop an index",
      response = SuccessResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/{indexName}")
  public Response dropIndex(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("indexName")
          final String indexName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          Optional<Table> table =
              keyspace.tables().stream()
                  .filter(t -> t.indexes().stream().anyMatch(i -> indexName.equals(i.name())))
                  .findFirst();

          if (!table.isPresent()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("Index '%s' not found.", indexName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          dropIndex(keyspaceName, indexName, authenticatedDB, table);

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  private void dropIndex(
      String keyspaceName, String indexName, AuthenticatedDB authenticatedDB, Optional<Table> table)
      throws UnauthorizedException, InterruptedException, java.util.concurrent.ExecutionException {
    db.getAuthorizationService()
        .authorizeSchemaWrite(
            authenticatedDB.getAuthenticationSubject(),
            keyspaceName,
            table.get().name(),
            Scope.DROP,
            SourceAPI.REST);

    authenticatedDB
        .getDataStore()
        .queryBuilder()
        .drop()
        .index(keyspaceName, indexName)
        .build()
        .execute()
        .get();
  }
}
