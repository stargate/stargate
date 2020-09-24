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
package io.stargate.health;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/checker")
@Produces(MediaType.APPLICATION_JSON)
public class CheckerResource {
  private static final Logger logger = LoggerFactory.getLogger(CheckerResource.class);

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
