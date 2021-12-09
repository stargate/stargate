package io.stargate.sgv2.restsvc.resources.schemas;

import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoTypeTranslator;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import io.stargate.sgv2.restsvc.resources.Sgv2RequestHandler;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2ColumnsResourceImpl extends ResourceBase implements Sgv2ColumnsResourceApi {
  @Override
  public Response getAllColumns(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      boolean raw,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      return jaxrsBadRequestError("keyspaceName must be provided").build();
    }
    if (isStringEmpty(tableName)) {
      return jaxrsBadRequestError("table name must be provided").build();
    }
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          Schema.CqlTable tableDef =
              BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
          List<Sgv2ColumnDefinition> columns = table2columns(tableDef);
          final Object payload = raw ? columns : new Sgv2RESTResponse(columns);
          return jaxrsResponse(Response.Status.OK).entity(payload).build();
        });
  }

  @Override
  public Response createColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      Sgv2ColumnDefinition columnDefinition,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response getOneColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      boolean raw,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response updateColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      Sgv2ColumnDefinition columnUpdate,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response deleteColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for converting b/w Bridge (proto) and REST (POJO) types
  /////////////////////////////////////////////////////////////////////////
   */

  private List<Sgv2ColumnDefinition> table2columns(Schema.CqlTable grpcTable) {
    final List<Sgv2ColumnDefinition> columns = new ArrayList<>();
    columns2columns(grpcTable.getPartitionKeyColumnsList(), columns, false);
    columns2columns(grpcTable.getClusteringKeyColumnsList(), columns, false);
    columns2columns(grpcTable.getStaticColumnsList(), columns, true);
    columns2columns(grpcTable.getColumnsList(), columns, false);
    return columns;
  }

  private void columns2columns(
      List<QueryOuterClass.ColumnSpec> columnsIn,
      List<Sgv2ColumnDefinition> columnsOut,
      boolean isStatic) {
    for (QueryOuterClass.ColumnSpec column : columnsIn) {
      columnsOut.add(column2column(column, isStatic));
    }
  }

  private Sgv2ColumnDefinition column2column(QueryOuterClass.ColumnSpec column, boolean isStatic) {
    return new Sgv2ColumnDefinition(
        column.getName(),
        BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(column.getType()),
        isStatic);
  }
}
