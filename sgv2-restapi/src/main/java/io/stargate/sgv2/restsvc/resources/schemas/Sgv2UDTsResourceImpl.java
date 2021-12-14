package io.stargate.sgv2.restsvc.resources.schemas;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restsvc.models.Sgv2UDTAddRequest;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
@Singleton
@CreateGrpcStub
public class Sgv2UDTsResourceImpl extends ResourceBase implements Sgv2UDTsResourceApi {
  @Override
  public Response findAll(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final boolean raw,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);

    // !!! TO IMPLEMENT
    return Response.status(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response findById(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String typeName,
      final boolean raw,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);

    // !!! TO IMPLEMENT
    return Response.status(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response createType(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final Sgv2UDTAddRequest udtAdd,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);
    final String typeName = udtAdd.getName();
    requireNonEmptyTypename(typeName);

    List<Column> columns = Arrays.asList();

    String cql =
        new QueryBuilder()
            .create()
            .type(keyspaceName, typeName)
            .ifNotExists()
            .column(columns)
            .build();

    blockingStub.executeQuery(
        QueryOuterClass.Query.newBuilder()
            .setParameters(parametersForLocalQuorum())
            .setCql(cql)
            .build());

    // !!! TO IMPLEMENT
    return Response.status(Response.Status.OK)
        .entity(Collections.singletonMap("name", typeName))
        .build();
  }

  @Override
  public Response delete(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String typeName,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);

    // !!! TO IMPLEMENT
    return Response.status(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response update(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final UserDefinedTypeUpdate udtUpdate,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);

    // !!! TO IMPLEMENT
    return Response.status(Response.Status.NOT_IMPLEMENTED).build();
  }

  private static void requireNonEmptyTypename(String typeName) {
    if (isStringEmpty(typeName)) {
      throw new WebApplicationException("Type name must be provided", Response.Status.BAD_REQUEST);
    }
  }
}
