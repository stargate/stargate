package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.docsapi.config.StargateConfig;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/hello")
@SecurityRequirement(name = StargateConfig.Constants.OPEN_API_DEFAULT_SECURITY_SCHEME)
public class GreetingResource {

    @Inject
    GrpcClients grpcClients;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return grpcClients.bridgeClient().getClass().getSimpleName();
    }
}