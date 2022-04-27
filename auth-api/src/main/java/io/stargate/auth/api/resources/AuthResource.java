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
package io.stargate.auth.api.resources;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.model.AuthApiError;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Secret;
import io.stargate.auth.model.UsernameCredentials;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"auth"})
@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class AuthResource {

  private static final Logger logger = LoggerFactory.getLogger(AuthResource.class);

  // The token's Time To Live in seconds
  private static final int tokenMaxAge =
      Integer.parseInt(System.getProperty("stargate.auth_tokenttl", "1800"));
  private final AuthenticationService authService;
  private final boolean shouldEnableUsernameToken;
  private final CacheControl cacheControlNoStore = cacheControlNoStore();
  private final CacheControl cacheControlWithMaxAge = cacheControlWithAge();

  @Inject
  public AuthResource(AuthenticationService authService) {
    this(authService, Boolean.getBoolean("stargate.auth_api_enable_username_token"));
  }

  public AuthResource(AuthenticationService authService, boolean shouldEnableUsernameToken) {
    this.authService = authService;
    this.shouldEnableUsernameToken = shouldEnableUsernameToken;
  }

  @POST
  @Path("/auth/token/generate")
  @ApiOperation(
      value = "Generate Token",
      notes = "Generate an authorization token to authenticate and perform requests.",
      response = AuthTokenResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "resource created", response = AuthTokenResponse.class),
        @ApiResponse(code = 400, message = "Bad Request", response = AuthApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = AuthApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = AuthApiError.class)
      })
  public Response createToken(
      @ApiParam(value = "", required = true) Secret secret, @Context HttpServletRequest request) {
    if (secret == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide a body to the request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (secret.getKey() == null || secret.getKey().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide key in request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (secret.getSecret() == null || secret.getSecret().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide secret in request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    String token;
    try {
      token = authService.createToken(secret.getKey(), secret.getSecret(), getAllHeaders(request));
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .cacheControl(cacheControlWithMaxAge)
        .build();
  }

  @POST
  @Path("/auth")
  @ApiOperation(
      value = "Create Token",
      notes = "Create an authorization token to authenticate and perform requests.",
      response = AuthTokenResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "resource created", response = AuthTokenResponse.class),
        @ApiResponse(code = 400, message = "Bad Request", response = AuthApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = AuthApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = AuthApiError.class)
      })
  public Response createToken(
      @ApiParam(value = "", required = true) Credentials credentials,
      @Context HttpServletRequest request) {
    if (credentials == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide a body to the request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (credentials.getUsername() == null || credentials.getUsername().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide username in request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (credentials.getPassword() == null || credentials.getPassword().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide password in request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    String token;
    try {
      token =
          authService.createToken(
              credentials.getUsername(), credentials.getPassword(), getAllHeaders(request));
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token, internal error: " + e, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .cacheControl(cacheControlWithMaxAge)
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
  @Path("/admin/auth/usernametoken")
  @ApiOperation(
      value = "Create Token from Username",
      notes =
          "Generate an authorization token to authenticate and perform requests. \n NOTE: This method is "
              + "intended to be used in conjunction with another authentication service. For example, "
              + "the request has already been authenticated through a proxy but a token is still needed "
              + "to make requests to Stargate.",
      response = AuthTokenResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "resource created", response = AuthTokenResponse.class),
        @ApiResponse(code = 400, message = "Bad Request", response = AuthApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = AuthApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = AuthApiError.class)
      })
  public Response createTokenFromUsername(
      @ApiParam(value = "", required = true) UsernameCredentials usernameCredentials,
      @Context HttpServletRequest request) {
    if (!shouldEnableUsernameToken) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Generating a token for a username is not allowed"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (usernameCredentials == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide a body to the request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    if (usernameCredentials.getUsername() == null || usernameCredentials.getUsername().equals("")) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new AuthApiError("Must provide username in request"))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    String token;
    try {
      token = authService.createToken(usernameCredentials.getUsername(), getAllHeaders(request));
    } catch (UnauthorizedException e) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    } catch (Exception e) {
      logger.error("Failed to create token", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new AuthApiError("Failed to create token: " + e.getMessage()))
          .cacheControl(cacheControlNoStore)
          .build();
    }

    return Response.status(Response.Status.CREATED)
        .entity(new AuthTokenResponse().authToken(token))
        .cacheControl(cacheControlWithMaxAge)
        .build();
  }

  /**
   * Used to create a Cache-Control header containing the proper values for the token's max-age and
   * s-maxage as defined by {@code stargate.auth_tokenttl} minus some time to avoid expiration. If
   * the property is not set then the default value of 1790 will be used.
   *
   * @return A {@link CacheControl} object which is converted into a Cache-Control header with
   *     proper directives.
   */
  private CacheControl cacheControlWithAge() {
    // For a large enough TTL it's safe to knock off 10 seconds but for a smaller number reducing by
    // that much will negatively impact caching so instead subtract only 1 second.
    int maxAge = tokenMaxAge <= 100 ? tokenMaxAge - 1 : tokenMaxAge - 10;
    CacheControl cacheControl = new CacheControl();
    cacheControl.setMaxAge(maxAge);
    cacheControl.setSMaxAge(maxAge);

    return cacheControl;
  }

  /**
   * Used to create a Cache-Control header that will indicate the response should not be cached.
   * This is intended to be used in the event of a failure to authenticate.
   *
   * @return A {@link CacheControl} object denoting the response should not be cached.
   */
  private CacheControl cacheControlNoStore() {
    CacheControl cacheControl = new CacheControl();
    cacheControl.setMaxAge(0);
    cacheControl.setSMaxAge(0);
    cacheControl.setNoStore(true);

    return cacheControl;
  }

  // non-private for test access
  static Map<String, String> getAllHeaders(HttpServletRequest request) {
    if (request == null) {
      return Collections.emptyMap();
    }
    final Map<String, String> allHeaders = new HashMap<>();
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerName = headerNames.nextElement();
      allHeaders.put(headerName, request.getHeader(headerName));
    }
    return allHeaders;
  }
}
