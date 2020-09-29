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
import io.stargate.db.schema.Table;
import io.stargate.web.models.ClusteringExpression;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.SuccessResponse;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableOptions;
import io.stargate.web.models.TableResponse;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/keyspaces/{keyspaceName}/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  private static final Logger logger = LoggerFactory.getLogger(TableResource.class);

  @Inject private Db db;

  @Timed
  @GET
  public Response listAll(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          return Response.status(Response.Status.OK)
              .entity(
                  db.getTables(localDB, keyspaceName).stream()
                      .map(Table::name)
                      .collect(Collectors.toList()))
              .build();
        });
  }

  @Timed
  @POST
  public Response create(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @NotNull final TableAdd tableAdd) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          if (tableAdd.getName() == null || tableAdd.getName().equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new Error("table name must be provided"))
                .build();
          }

          if (tableAdd.getPrimaryKey() == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new Error("primary key must be provided"))
                .build();
          }

          String createStmt = "CREATE TABLE";
          if (tableAdd.getIfNotExists()) {
            createStmt += " IF NOT EXISTS";
          }

          StringBuilder columnDefinitions = new StringBuilder("(");
          for (ColumnDefinition colDef : tableAdd.getColumnDefinitions()) {
            if (colDef.getName() == null || colDef.getName().equals("")) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(new Error("column name must be provided"))
                  .build();
            }
            columnDefinitions
                .append(Converters.maybeQuote(colDef.getName()))
                .append(" ")
                .append(colDef.getTypeDefinition());
            if (colDef.getIsStatic()) {
              columnDefinitions.append(" STATIC");
            }

            columnDefinitions.append(", ");
          }

          String primaryKey =
              "(" + String.join(", ", tableAdd.getPrimaryKey().getPartitionKey()) + ")";
          if (tableAdd.getPrimaryKey().getClusteringKey().size() > 0) {
            String clusteringKey = String.join(", ", tableAdd.getPrimaryKey().getClusteringKey());
            primaryKey = "(" + primaryKey + ", " + clusteringKey + ")";
          }

          columnDefinitions.append("PRIMARY KEY ").append(primaryKey).append(")");

          String tableOptions;
          try {
            tableOptions = Converters.getTableOptions(tableAdd);
          } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "Unable to create table options " + e.getMessage(),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          String query =
              String.format(
                  "%s %s.%s %s %s",
                  createStmt,
                  Converters.maybeQuote(keyspaceName),
                  Converters.maybeQuote(tableAdd.getName()),
                  columnDefinitions.toString(),
                  tableOptions);
          localDB
              .query(
                  query.trim(), Optional.of(ConsistencyLevel.LOCAL_QUORUM), Collections.emptyList())
              .get();

          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @GET
  @Path("/{tableName}")
  public Response getOne(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          final List<ColumnDefinition> columnDefinitions =
              tableMetadata.columns().stream()
                  .map(
                      (col) ->
                          new ColumnDefinition(
                              col.name(),
                              Objects.requireNonNull(col.type()).isParameterized()
                                  ? null
                                  : Objects.requireNonNull(col.type()).name(),
                              col.kind() == Kind.Static))
                  .collect(Collectors.toList());

          final List<String> partitionKey =
              tableMetadata.partitionKeyColumns().stream()
                  .map(Column::name)
                  .collect(Collectors.toList());
          final List<String> clusteringKey =
              tableMetadata.clusteringKeyColumns().stream()
                  .map(Column::name)
                  .collect(Collectors.toList());
          final List<ClusteringExpression> clusteringExpression =
              tableMetadata.clusteringKeyColumns().stream()
                  .map(
                      (col) ->
                          new ClusteringExpression(
                              col.name(), Objects.requireNonNull(col.order()).name()))
                  .collect(Collectors.toList());

          final PrimaryKey primaryKey = new PrimaryKey(partitionKey, clusteringKey);
          final int ttl = 0; // no way to get this without querying schema currently
          final TableOptions tableOptions = new TableOptions(ttl, clusteringExpression);

          return Response.ok(
                  new TableResponse(
                      tableMetadata.name(),
                      keyspaceName,
                      columnDefinitions,
                      primaryKey,
                      tableOptions))
              .build();
        });
  }

  @Timed
  @DELETE
  @Path("/{tableName}")
  public Response delete(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          localDB
              .query()
              .drop()
              .table(keyspaceName, tableName)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }
}
