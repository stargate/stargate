package io.stargate.health;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Handles the two application resources under the '/checker/' path.
 *
 * <p>Note that they are a bit redundant with regular DropWizard health checks under '/healthcheck',
 * but we preserve them because they were there first and some tools depend on them.
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class CheckerResource {

  @Inject private BundleService bundleService;

  @GET
  @Path("/liveness")
  public Response checkLiveness() {
    // Sufficient to just return a 200 OK since liveness just means "app is up and doesn't need
    // restarted"
    if (bundleService.checkBundleStates()) {
      return Response.status(Response.Status.OK).entity("UP").build();
    }

    return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("DOWN").build();
  }

  @GET
  @Path("/readiness")
  public Response checkReadiness() {
    // Readiness requires a more heavy weight check to decide if the app is able to successfully
    // respond to traffic
    if (bundleService.checkIsReady()) {
      return Response.status(Response.Status.OK).entity("READY").build();
    }
    return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("NOT READY").build();
  }
}
