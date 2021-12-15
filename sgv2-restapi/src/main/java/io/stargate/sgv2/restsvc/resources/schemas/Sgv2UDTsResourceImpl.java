package io.stargate.sgv2.restsvc.resources.schemas;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import io.grpc.StatusRuntimeException;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restsvc.models.Sgv2UDT;
import io.stargate.sgv2.restsvc.models.Sgv2UDTAddRequest;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
@Singleton
@CreateGrpcStub
public class Sgv2UDTsResourceImpl extends ResourceBase implements Sgv2UDTsResourceApi {
  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

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
    requireNonEmptyTypename(typeName);

    /*
        String cql =
                new QueryBuilder()
                        .create()
                        .type(keyspaceName, typeName)
                        .column(columns2columns(udtAdd.getFields()))
                        .build();
        QueryOuterClass.Query query =
                QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql).build();
        QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    //    final Object payload = raw ? tableResponses : new Sgv2RESTResponse(tableResponses);
    //    return Response.status(Response.Status.OK).entity(payload).build();
         */

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

    String cql =
        new QueryBuilder()
            .create()
            .type(keyspaceName, typeName)
            .ifNotExists(udtAdd.getIfNotExists())
            .column(columns2columns(udtAdd.getFields()))
            .build();

    logger.info("createUDT() with CQL: {}", cql);

    try {
      blockingStub.executeQuery(
          QueryOuterClass.Query.newBuilder()
              .setParameters(parametersForLocalQuorum())
              .setCql(cql)
              .build());
    } catch (StatusRuntimeException grpcE) {
      // For most failures pass and let default handler deal; but for specific case of
      // trying to create existing UDT without "if-not-exists", try to dig actual fail
      // message
      switch (grpcE.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          final String desc = grpcE.getStatus().getDescription();
          if (desc.contains("already exists")) {
            return restServiceError(Response.Status.BAD_REQUEST, "Bad request: " + desc);
          }
      }
      // otherwise just pass it along
      throw grpcE;
    }

    return Response.status(Response.Status.CREATED)
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
    requireNonEmptyTypename(typeName);

    String cql =
        new QueryBuilder()
            .drop()
            .type(keyspaceName, typeName)
            // 15-Dec-2021, tatu: Why no "ifExists" option? SGv1 had none which
            //    seems inconsistent; would be good for idempotency
            // .ifExists()
            .build();
    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    return Response.status(Response.Status.NO_CONTENT).build();
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

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private List<Column> columns2columns(List<Sgv2UDT.UDTField> fields) {
    List<Column> result = new ArrayList<>();
    for (Sgv2UDT.UDTField colDef : fields) {
      String columnName = colDef.getName();
      String typeDef = colDef.getTypeDefinition();
      if (isStringEmpty(columnName) || isStringEmpty(typeDef)) {
        throw new WebApplicationException(
            "Field name and type definition must be provided", Response.Status.BAD_REQUEST);
      }
      result.add(
          ImmutableColumn.builder()
              .name(columnName)
              .kind(Column.Kind.REGULAR)
              .type(typeDef)
              .build());
    }
    if (result.isEmpty()) {
      throw new WebApplicationException(
          "There should be at least one field defined", Response.Status.BAD_REQUEST);
    }
    return result;
  }

  private static void requireNonEmptyTypename(String typeName) {
    if (isStringEmpty(typeName)) {
      throw new WebApplicationException("Type name must be provided", Response.Status.BAD_REQUEST);
    }
  }
}
