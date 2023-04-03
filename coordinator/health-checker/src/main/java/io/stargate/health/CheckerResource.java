package io.stargate.health;

import static io.stargate.health.HealthCheckerActivator.BUNDLES_CHECK_NAME;
import static io.stargate.health.HealthCheckerActivator.SCHEMA_CHECK_NAME;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheck.Result;
import com.codahale.metrics.health.HealthCheckRegistry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the two application resources under the '/checker/' path.
 *
 * <p>Note that they are a bit redundant with regular DropWizard health checks under '/healthcheck',
 * but we preserve them because they were there first and some tools depend on them.
 */
@Path("/checker")
@Produces(MediaType.TEXT_PLAIN)
@Singleton
public class CheckerResource {

  private static final Logger logger = LoggerFactory.getLogger(CheckerResource.class);

  @Inject private BundleService bundleService;
  @Inject private HealthCheckRegistry healthCheckRegistry;

  public CheckerResource() {
    // for production use with dependency injection
  }

  // for testing only
  CheckerResource(BundleService bundleService, HealthCheckRegistry healthCheckRegistry) {
    this.bundleService = bundleService;
    this.healthCheckRegistry = healthCheckRegistry;
  }

  @GET
  @Path("/liveness")
  public Response checkLiveness() {
    // Sufficient to just return a 200 OK since liveness just means "app is up and doesn't need
    // restarted"
    HealthCheck bundleHealthCheck = healthCheckRegistry.getHealthCheck(BUNDLES_CHECK_NAME);
    if (bundleHealthCheck == null || !bundleHealthCheck.execute().isHealthy()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("DOWN").build();
    }

    HealthCheck schemaHealthCheck = healthCheckRegistry.getHealthCheck(SCHEMA_CHECK_NAME);
    if (schemaHealthCheck == null || !schemaHealthCheck.execute().isHealthy()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("DOWN").build();
    }

    return Response.status(Response.Status.OK).entity("UP").build();
  }

  @GET
  @Path("/readiness")
  public Response checkReadiness(@QueryParam("check") Set<String> checks) {
    boolean checkAll = checks == null || checks.isEmpty();

    HashSet<String> requiredChecks =
        new HashSet<>(checkAll ? bundleService.defaultHealthCheckNames() : checks);

    SortedMap<String, Result> status =
        healthCheckRegistry.runHealthChecks(
            (name, healthCheck) -> checkAll || requiredChecks.contains(name));

    boolean ready = true;

    for (Entry<String, Result> e : status.entrySet()) {
      String name = e.getKey();
      requiredChecks.remove(name);
      boolean healthy = e.getValue().isHealthy();

      if (!healthy) {
        logger.warn("Failed health check: {}", name);
        ready = false;
      }
    }

    if (!requiredChecks.isEmpty()) {
      logger.warn("Missing required health checks: {}", requiredChecks);
      ready = false;
    }

    if (ready) {
      return Response.status(Response.Status.OK).entity("READY").build();
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("NOT READY").build();
    }
  }
}
