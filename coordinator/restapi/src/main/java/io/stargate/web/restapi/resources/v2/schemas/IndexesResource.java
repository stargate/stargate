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
package io.stargate.web.restapi.resources.v2.schemas;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.util.Strings;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Predicate;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.models.ApiError;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.RequestHandler;
import io.stargate.web.restapi.dao.RestDB;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.models.IndexAdd;
import io.stargate.web.restapi.models.IndexKind;
import io.stargate.web.restapi.models.SuccessResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
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
import org.apache.cassandra.stargate.db.ConsistencyLevel;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/indexes")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class IndexesResource {
  @Inject private RestDBFactory dbFactory;

  private final String SYSTEM_SCHEMA = "system_schema";
  private final String INDEXES_TABLE = "indexes";

  @Timed
  @GET
  @ApiOperation(
      value = "Get all indexes for a given table",
      notes = "Get all indexes for a given table",
      response = SuccessResponse.class,
      code = 200)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  public Response getAllIndexesForTable(
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
          @PathParam("tableName")
          final String tableName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          restDB.authorizeDataRead(keyspaceName, tableName, SourceAPI.REST);

          try {
            AbstractTable tableMetadata = restDB.getTable(SYSTEM_SCHEMA, INDEXES_TABLE);
            List<Column> columns = tableMetadata.columns();
            BoundQuery query =
                restDB
                    .queryBuilder()
                    .select()
                    .column(columns)
                    .from(SYSTEM_SCHEMA, INDEXES_TABLE)
                    .where("table_name", Predicate.EQ, tableName)
                    .where("keyspace_name", Predicate.EQ, keyspaceName)
                    .allowFiltering(true)
                    .build()
                    .bind();

            final ResultSet r =
                restDB.authorizedDataRead(
                    () -> restDB.execute(query, ConsistencyLevel.LOCAL_QUORUM).get(),
                    keyspaceName,
                    tableName,
                    TypedKeyValue.forSelect((BoundSelect) query),
                    SourceAPI.REST);

            List<Map<String, Object>> rows =
                r.currentPageRows().stream().map(Converters::row2Map).collect(Collectors.toList());
            return Response.status(Response.Status.OK)
                .entity(Converters.writeResponse(rows))
                .build();
          } catch (NotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new ApiError(
                        String.format("Table '%s' not found in keyspace.", tableName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }
        });
  }

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
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
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
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(required = true) @NotNull final IndexAdd indexAdd,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.CREATE, SourceAPI.REST, ResourceKind.INDEX);

          String columnName = indexAdd.getColumn();
          if (Strings.isNullOrEmpty(columnName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        String.format("Column name ('%s') cannot be empty/null.", columnName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          Keyspace keyspace = restDB.getKeyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new ApiError(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          try {
            final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);
            final Column col = tableMetadata.column(columnName);
            if (col == null) {
              return Response.status(Response.Status.NOT_FOUND)
                  .entity(
                      new ApiError(
                          String.format("Column '%s' not found in table.", columnName),
                          Response.Status.NOT_FOUND.getStatusCode()))
                  .build();
            }
          } catch (NotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new ApiError(
                        String.format("Table '%s' not found in keyspace.", tableName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          boolean indexKeys = indexAdd.getKind() == IndexKind.KEYS;
          boolean indexEntries = indexAdd.getKind() == IndexKind.ENTRIES;
          boolean indexValues = indexAdd.getKind() == IndexKind.VALUES;
          boolean indexFull = indexAdd.getKind() == IndexKind.FULL;

          CollectionIndexingType indexingType =
              ImmutableCollectionIndexingType.builder()
                  .indexEntries(indexEntries)
                  .indexKeys(indexKeys)
                  .indexValues(indexValues)
                  .indexFull(indexFull)
                  .build();

          restDB
              .queryBuilder()
              .create()
              .index(indexAdd.getName())
              .ifNotExists(indexAdd.getIfNotExists())
              .on(keyspaceName, tableName)
              .column(columnName)
              .indexingType(indexingType)
              .custom(indexAdd.getType(), indexAdd.getOptions())
              .build()
              .execute()
              .get();

          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(
      value = "Drop an index from keyspace",
      notes = "Drop an index",
      response = SuccessResponse.class,
      code = 204)
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
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
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "Name of the index to use for the request.", required = true)
          @PathParam("indexName")
          final String indexName,
      @ApiParam(
              defaultValue = "false",
              value =
                  "If the index doesn't exists drop will throw an error unless this query param is set to true.")
          @QueryParam("ifExists")
          final boolean ifExists,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.DROP, SourceAPI.REST, ResourceKind.INDEX);

          Keyspace keyspace = restDB.getKeyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new ApiError(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          try {
            final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);
            Index index =
                tableMetadata instanceof Table ? ((Table) tableMetadata).index(indexName) : null;
            if (index == null && !ifExists) {
              return Response.status(Response.Status.NOT_FOUND)
                  .entity(
                      new ApiError(
                          String.format("Index '%s' not found.", indexName),
                          Response.Status.NOT_FOUND.getStatusCode()))
                  .build();
            }
          } catch (NotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new ApiError(
                        String.format("Table '%s' not found in keyspace.", tableName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          restDB
              .queryBuilder()
              .drop()
              .index(keyspaceName, indexName)
              .ifExists(ifExists)
              .build()
              .execute()
              .get();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }
}
