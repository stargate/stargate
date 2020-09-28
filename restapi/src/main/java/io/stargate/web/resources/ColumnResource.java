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
package io.stargate.web.resources;

import com.codahale.metrics.annotation.Timed;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Table;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.ColumnUpdate;
import io.stargate.web.models.Error;
import io.stargate.web.models.SuccessResponse;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnResource {
  private static final Logger logger = LoggerFactory.getLogger(ColumnResource.class);

  @Inject private Db db;

  @Timed
  @GET
  public Response getAll(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          List<ColumnDefinition> collect =
              tableMetadata.columns().stream()
                  .map(
                      (col) -> {
                        String type = col.type() == null ? null : col.type().cqlDefinition();
                        return new ColumnDefinition(
                            col.name(), type, col.kind() == Column.Kind.Static);
                      })
                  .collect(Collectors.toList());

          return Response.status(Response.Status.OK).entity(collect).build();
        });
  }

  @Timed
  @POST
  public Response addColumn(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @NotNull final ColumnDefinition columnDefinition) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          String name = columnDefinition.getName();
          Kind kind = Kind.Regular;
          if (columnDefinition.getIsStatic()) {
            kind = Kind.Static;
          }

          Column column =
              ImmutableColumn.builder()
                  .name(name)
                  .kind(kind)
                  .type(Column.Type.fromCqlDefinitionOf(columnDefinition.getTypeDefinition()))
                  .build();

          localDB
              .query()
              .alter()
              .table(keyspaceName, tableName)
              .addColumn(column)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @GET
  @Path("/{columnName}")
  public Response getOne(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("columnName") final String columnName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);
          final Column col = tableMetadata.column(columnName);
          if (col == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(new Error(String.format("column '%s' not found in table", columnName)))
                .build();
          }

          String type = col.type() == null ? null : col.type().cqlDefinition();
          return Response.status(Response.Status.OK)
              .entity(new ColumnDefinition(col.name(), type, col.kind() == Kind.Static))
              .build();
        });
  }

  @Timed
  @DELETE
  @Path("/{columnName}")
  public Response delete(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("columnName") final String columnName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          localDB
              .query()
              .alter()
              .table(keyspaceName, tableName)
              .dropColumn(columnName)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @PUT
  @Path("/{columnName}")
  public Response update(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("columnName") final String columnName,
      @NotNull final ColumnUpdate columnUpdate) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          String alterInstructions =
              "RENAME "
                  + Converters.maybeQuote(columnName)
                  + " TO "
                  + Converters.maybeQuote(columnUpdate.getNewName());
          localDB
              .query(
                  String.format(
                      "ALTER TABLE %s.%s %s",
                      Converters.maybeQuote(keyspaceName),
                      Converters.maybeQuote(tableName),
                      alterInstructions),
                  Optional.of(ConsistencyLevel.LOCAL_QUORUM),
                  Collections.emptyList())
              .get();

          return Response.status(Response.Status.OK).entity(new SuccessResponse()).build();
        });
  }
}
