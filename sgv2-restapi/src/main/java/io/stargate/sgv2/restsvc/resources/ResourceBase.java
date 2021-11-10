package io.stargate.sgv2.restsvc.resources;

import io.stargate.sgv2.restsvc.models.RestServiceError;
import javax.ws.rs.core.Response;

/**
 * Base class for resource classes; contains utility/helper methods for which there is no more
 * specific place.
 */
public abstract class ResourceBase {
  protected static Response invalidTokenFailure() {
    return Response.status(Response.Status.UNAUTHORIZED)
        .entity(
            new RestServiceError(
                "Missing or invalid Auth Token", Response.Status.UNAUTHORIZED.getStatusCode()))
        .build();
  }

  /**
   * Method that checks for some common types of invalidity for Auth Token: currently simply its
   * existence (cannot be {@code null}) or empty ({code !string.isEmpty()}), but may be extended
   * with heuristics in future.
   */
  protected static final boolean isAuthTokenInvalid(String authToken) {
    return isStringEmpty(authToken);
  }

  protected static final boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }
}
