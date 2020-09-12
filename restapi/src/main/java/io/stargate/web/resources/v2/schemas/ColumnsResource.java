package io.stargate.web.resources.v2.schemas;

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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.Timed;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.ImmutableColumn;
import io.stargate.db.datastore.schema.Table;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnsResource {
    private static final Logger logger = LoggerFactory.getLogger(ColumnsResource.class);

    @Inject
    private Db db;

    @Timed
    @GET
    public Response getAll(@HeaderParam("X-Cassandra-Token") String token, @PathParam("keyspaceName") final String keyspaceName,
                           @PathParam("tableName") final String tableName, @QueryParam("pretty") final boolean pretty,
                           @QueryParam("raw") final boolean raw) {
        return RequestHandler.handle(() -> {
            DataStore localDB = db.getDataStoreForToken(token);

            final Table tableMetadata;
            try {
                tableMetadata = db.getTable(localDB, keyspaceName, tableName);
            } catch (Exception e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new Error(String.format("table '%s' not found", tableName), Response.Status.BAD_REQUEST.getStatusCode()))
                        .build();
            }

            List<ColumnDefinition> columnDefinitions = tableMetadata.columns().stream()
                    .map((col) -> new ColumnDefinition(col.name(), col.type().name(), col.kind() == Column.Kind.Static))
                    .collect(Collectors.toList());

            Object response = raw ? columnDefinitions : new ResponseWrapper(columnDefinitions);
            return Response
                    .status(Response.Status.OK)
                    .entity(Converters.writeResponse(response, pretty))
                    .build();
        });
    }

    @Timed
    @POST
    public Response addColumn(@HeaderParam("X-Cassandra-Token") String token, @PathParam("keyspaceName") final String keyspaceName,
                              @PathParam("tableName") final String tableName, @QueryParam("pretty") final boolean pretty,
                              @NotNull final ColumnDefinition columnDefinition) {
        return RequestHandler.handle(() -> {
            DataStore localDB = db.getDataStoreForToken(token);

            String name = columnDefinition.getName();
            Column.Kind kind = Column.Kind.Regular;
            if (columnDefinition.getIsStatic()) {
                kind = Column.Kind.Static;
            }

            Column column = ImmutableColumn.builder()
                    .name(name)
                    .kind(kind)
                    .type(Column.Type.fromCqlDefinitionOf(columnDefinition.getTypeDefinition()))
                    .build();

            localDB.query().alter().table(keyspaceName, tableName).addColumn(column)
                    .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM).execute();

            return Response
                    .status(Response.Status.CREATED)
                    .entity(Converters.writeResponse(Collections.singletonMap("name", columnDefinition.getName()), pretty))
                    .build();
        });
    }

    @Timed
    @GET
    @Path("/{columnName}")
    public Response getOne(@HeaderParam("X-Cassandra-Token") String token, @PathParam("keyspaceName") final String keyspaceName,
                           @PathParam("tableName") final String tableName, @PathParam("columnName") final String columnName,
                           @QueryParam("pretty") final boolean pretty, @QueryParam("raw") final boolean raw) {
        return RequestHandler.handle(() -> {
            DataStore localDB = db.getDataStoreForToken(token);

            final Table tableMetadata;
            try {
                tableMetadata = db.getTable(localDB, keyspaceName, tableName);
            } catch (Exception e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new Error(String.format("table '%s' not found", tableName), Response.Status.BAD_REQUEST.getStatusCode()))
                        .build();
            }

            final Column col = tableMetadata.column(columnName);
            if (col == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new Error(String.format("column '%s' not found in table", columnName), Response.Status.NOT_FOUND.getStatusCode())).build();
            }

            ColumnDefinition columnDefinition = new ColumnDefinition(col.name(), col.type().name(), col.kind() == Column.Kind.Static);
            Object response = raw ? columnDefinition : new ResponseWrapper(columnDefinition);
            return Response
                    .status(Response.Status.OK)
                    .entity(Converters.writeResponse(response, pretty))
                    .build();
        });
    }

    @Timed
    @PUT
    @Path("/{columnName}")
    public Response update(@HeaderParam("X-Cassandra-Token") String token, @PathParam("keyspaceName") final String keyspaceName,
                           @PathParam("tableName") final String tableName, @PathParam("columnName") final String columnName,
                           @QueryParam("pretty") final boolean pretty, @NotNull final ColumnDefinition columnUpdate) {
        return RequestHandler.handle(() -> {
            DataStore localDB = db.getDataStoreForToken(token);

            String alterInstructions = "RENAME " + Converters.maybeQuote(columnName) + " TO " + Converters.maybeQuote(columnUpdate.getName());
            localDB.query(String.format("ALTER TABLE %s.%s %s", Converters.maybeQuote(keyspaceName), Converters.maybeQuote(tableName), alterInstructions), Optional.of(ConsistencyLevel.LOCAL_QUORUM), Collections.emptyList())
                    .get();

            return Response
                    .status(Response.Status.OK)
                    .entity(Converters.writeResponse(Collections.singletonMap("name", columnUpdate.getName()), pretty))
                    .build();
        });
    }

    @Timed
    @DELETE
    @Path("/{columnName}")
    public Response delete(@HeaderParam("X-Cassandra-Token") String token, @PathParam("keyspaceName") final String keyspaceName,
                           @PathParam("tableName") final String tableName, @PathParam("columnName") final String columnName) {
        return RequestHandler.handle(() -> {
            DataStore localDB = db.getDataStoreForToken(token);

            localDB.query().alter().table(keyspaceName, tableName).dropColumn(columnName)
                    .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM).execute();

            return Response.status(Response.Status.NO_CONTENT).build();
        });
    }
}
