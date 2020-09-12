package io.stargate.web.resources;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.Timed;
import io.stargate.db.datastore.DataStore;

@Path("/v1/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
public class KeyspaceResource {
  private static final Logger logger = LoggerFactory.getLogger(KeyspaceResource.class);

  @Inject
  private Db db;

  @Timed
  @GET
  public Response listAll(@HeaderParam("X-Cassandra-Token") String token) {
    return RequestHandler.handle(() -> {
      DataStore localDB = db.getDataStoreForToken(token);
      return Response.
              status(Response.Status.OK)
              .entity(localDB.schema().keyspaceNames())
              .build();
    });
  }
}