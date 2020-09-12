package io.stargate.auth.api;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Error;
import io.stargate.auth.model.Secret;

@Path("/v1")
public class AuthResource {
    private static final Logger logger = LoggerFactory.getLogger(AuthResource.class);

    final AuthenticationService authService;

    public AuthResource(AuthenticationService authService) {
        this.authService = authService;
    }

    @POST
    @Path("/auth/token/generate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createToken(@Valid Secret body) {
        if (body.getKey() == null || body.getKey().equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new Error("Must provide key in request"))
                    .build();
        }

        if (body.getSecret() == null || body.getSecret().equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new Error("Must provide secret in request"))
                    .build();
        }

        // TODO: [doug] 2020-06-18, Thu, 0:42 handle potential errors from authService 
        String token;
        try {
            token = authService.createToken(body.getKey(), body.getSecret());
        } catch (UnauthorizedException e) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity(new Error("Failed to create token: " + e.getMessage()))
                    .build();
        } catch (Exception e) {
            logger.error("Failed to create token", e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new Error("Failed to create token: " + e.getMessage()))
                    .build();
        }

        return Response
                .status(Response.Status.CREATED)
                .entity(new AuthTokenResponse().authToken(token))
                .build();
    }

    @POST
    @Path("/auth")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createToken(@Valid Credentials body) {
        if (body.getUsername() == null || body.getUsername().equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new Error("Must provide username in request"))
                    .build();
        }

        if (body.getPassword() == null || body.getPassword().equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new Error("Must provide password in request"))
                    .build();
        }

        // TODO: [doug] 2020-06-18, Thu, 0:42 handle potential errors from authService
        String token;
        try {
            token = authService.createToken(body.getUsername(), body.getPassword());
        } catch (UnauthorizedException e) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity(new Error("Failed to create token: " + e.getMessage()))
                    .build();
        } catch (Exception e) {
            logger.error("Failed to create token", e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new Error("Failed to create token: " + e.getMessage()))
                    .build();
        }

        return Response
                .status(Response.Status.CREATED)
                .entity(new AuthTokenResponse().authToken(token))
                .build();
    }

}