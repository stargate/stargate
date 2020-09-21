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
package io.stargate.auth.api;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Error;
import io.stargate.auth.model.Secret;
import io.stargate.auth.model.UsernameCredentials;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1")
public class AuthResource {

  private static final Logger logger = LoggerFactory.getLogger(AuthResource.class);

  final AuthenticationService authService;
  private static final boolean shouldEnableUsernameToken =
      Boolean.parseBoolean(System.getProperty("stargate.auth_api_enable_username_token", "false"));

  public AuthResource(AuthenticationService authService) {
    this.authService = authService;
  }

  @POST
  @Path("/auth/token/generate")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createToken(Secret secret) {
    if (secret == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide a body to the request"))
          .build();
    }

    if (secret.getKey() == null || secret.getKey().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide key in request"))
          .build();
    }

    if (secret.getSecret() == null || secret.getSecret().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide secret in request"))
          .build();
    }

    String token;
    try {
      token = authService.createToken(secret.getKey(), secret.getSecret());
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .build();
  }

  @POST
  @Path("/auth")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createToken(Credentials credentials) {
    if (credentials == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide a body to the request"))
          .build();
    }

    if (credentials.getUsername() == null || credentials.getUsername().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide username in request"))
          .build();
    }

    if (credentials.getPassword() == null || credentials.getPassword().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide password in request"))
          .build();
    }

    String token;
    try {
      token = authService.createToken(credentials.getUsername(), credentials.getPassword());
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .build();
  }

  /**
   * NOTE: This method is intended to be used in conjunction with another authentication service.
   * For example, the request has already been authenticated through a proxy but a token is still
   * needed to make requests to Stargate.
   *
   * <p>
   *
   * <p>Generates a token based solely on if the provided username exists in the database.
   *
   * @param usernameCredentials A username to generate a token for IFF the username exists in the
   *     database.
   * @return On success will return a token to be used with later requests to Stargate.
   */
  @POST
  @Path("/auth/usernametoken")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createTokenFromUsername(UsernameCredentials usernameCredentials) {
    if (!shouldEnableUsernameToken) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Generating a token for a username is not allowed"))
          .build();
    }

    if (usernameCredentials == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide a body to the request"))
          .build();
    }

    if (usernameCredentials.getUsername() == null || usernameCredentials.getUsername().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new Error("Must provide username in request"))
          .build();
    }

    String token;
    try {
      token = authService.createToken(usernameCredentials.getUsername());
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new Error("Failed to create token: " + e.getMessage()))
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .build();
  }
}
