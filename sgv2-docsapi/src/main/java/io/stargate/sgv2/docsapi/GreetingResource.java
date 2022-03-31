package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.docsapi.config.StargateConfig;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/hello")
@SecurityRequirement(name = StargateConfig.Constants.OPEN_API_DEFAULT_SECURITY_SCHEME)
public class GreetingResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello RESTEasy";
    }
}