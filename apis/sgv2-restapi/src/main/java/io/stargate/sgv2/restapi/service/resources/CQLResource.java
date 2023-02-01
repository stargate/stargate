package io.stargate.sgv2.restapi.service.resources;

import static io.stargate.sgv2.restapi.service.resources.RestResourceBase.convertRowsToResponse;
import static io.stargate.sgv2.restapi.service.resources.RestResourceBase.parametersForPageSizeStateAndKeyspace;

import io.quarkus.resteasy.reactive.server.EndpointDisabled;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2RowsResponse;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.validation.constraints.NotBlank;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

@ApplicationScoped
@Path("/v2/cql")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.TEXT_PLAIN)
@SecurityRequirement(name = RestOpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = RestOpenApiConstants.Tags.DATA)
@EndpointDisabled(
    name = "stargate.rest.cql.disabled",
    stringValue = "true",
    disableIfMissing = true)
public class CQLResource {
  @Inject protected StargateRequestInfo requestInfo;

  @POST
  @Operation(summary = "CQL Query", description = "Execute a cql query directly")
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description = "OK",
            content =
                @Content(
                    schema =
                        @Schema(
                            implementation = Sgv2RowsResponse.class,
                            type = SchemaType.OBJECT))),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = RestOpenApiConstants.Responses.GENERAL_500),
      })
  public Uni<RestResponse<Object>> cqlQuery(
      @Parameter(name = "keyspace", ref = RestOpenApiConstants.Parameters.KEYSPACE_AS_QUERY_PARAM)
          @QueryParam("keyspace")
          final String keyspace,
      @Parameter(name = "page-size", ref = RestOpenApiConstants.Parameters.PAGE_SIZE)
          @QueryParam("page-size")
          final int pageSizeParam,
      @Parameter(name = "page-state", ref = RestOpenApiConstants.Parameters.PAGE_STATE)
          @QueryParam("page-state")
          final String pageStateParam,
      @Parameter(name = "raw", ref = RestOpenApiConstants.Parameters.RAW) @QueryParam("raw")
          final boolean raw,
      @RequestBody(description = "CQL Query String", required = true)
          @NotBlank(message = "CQL query body required")
          final String payloadAsString) {
    QueryOuterClass.Query query =
        new QueryBuilder()
            .cql(payloadAsString)
            .parameters(
                parametersForPageSizeStateAndKeyspace(pageSizeParam, pageStateParam, keyspace))
            .build();
    return requestInfo
        .getStargateBridge()
        .executeQuery(query)
        .map(response -> convertRowsToResponse(response, raw));
  }
}
