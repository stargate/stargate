package io.stargate.web.docsapi.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.models.DocCollection;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Set;
import java.util.List;

@Api(
        produces = MediaType.APPLICATION_JSON,
        consumes = MediaType.APPLICATION_JSON,
        tags = {"documents"})
@Path("/v2/schemas/namespaces/{namespace-id: [a-zA-Z_0-9]+}")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource {
    private static final Logger logger = LoggerFactory.getLogger(CollectionsResource.class);

    @Inject
    private Db db;
    private static final ObjectMapper mapper = new ObjectMapper();

    @GET
    @ApiOperation(
            value = "List collections in namespace")
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
                    @ApiResponse(code = 401, message = "Unauthorized"),
                    @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
            })
    @Path("collections")
    @Consumes("application/json")
    @Produces("application/json")
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
            final boolean raw) {
        () -> {
            DataStore localDB = db.getDataStoreForToken(token);
            Set<Table> tables =
                    localDB.schema().keyspace(namespace).tables();
            tables.stream().map(table -> {
                if (db.isDse()) {
                    List<Index> indexes = table.indexes();
                } else {
                    return new DocCollection(table.name(), false, null);
                }
            })

            Object response = raw ? namespaces : new ResponseWrapper(namespaces);
            return Response.status(Response.Status.OK)
                    .entity(Converters.writeResponse(response))
                    .build();
        });
    }
}
