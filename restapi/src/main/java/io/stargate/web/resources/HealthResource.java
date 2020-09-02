package io.stargate.web.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class HealthResource {

    @GET
    @Path("/health")
    public Response health() {
        return Response
                .status(Response.Status.OK)
                .entity("UP")
                .build();
    }

    @GET
    public Response ping() {
        return Response
                .status(Response.Status.OK)
                .entity("It's Alive")
                .build();
    }
}
