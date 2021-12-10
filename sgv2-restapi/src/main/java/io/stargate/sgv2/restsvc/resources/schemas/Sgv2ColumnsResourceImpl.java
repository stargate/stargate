package io.stargate.sgv2.restsvc.resources.schemas;

import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoTypeTranslator;
import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2ColumnsResourceImpl extends ResourceBase implements Sgv2ColumnsResourceApi {
  // Singleton resource so no need to be static
  protected final Logger logger = LoggerFactory.getLogger(getClass());

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
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
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
    if (isStringEmpty(keyspaceName)) {
      return jaxrsBadRequestError("keyspaceName must be provided").build();
    }
    if (isStringEmpty(tableName)) {
      return jaxrsBadRequestError("table name must be provided").build();
    }
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
          // !!! TO BE IMPLEMENTED
          return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
        });
  }

  @Override
  public Response getOneColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      boolean raw,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      return jaxrsBadRequestError("keyspaceName must be provided").build();
    }
    if (isStringEmpty(tableName)) {
      return jaxrsBadRequestError("table name must be provided").build();
    }
    if (isStringEmpty(columnName)) {
      return jaxrsBadRequestError("columnName must be provided").build();
    }
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
          // !!! TO BE IMPLEMENTED
          return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
        });
  }

  @Override
  public Response updateColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      Sgv2ColumnDefinition columnUpdate,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      return jaxrsBadRequestError("keyspaceName must be provided").build();
    }
    if (isStringEmpty(tableName)) {
      return jaxrsBadRequestError("table name must be provided").build();
    }
    if (isStringEmpty(columnName)) {
      return jaxrsBadRequestError("columnName must be provided").build();
    }
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
          // !!! TO BE IMPLEMENTED
          return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
        });
  }

  @Override
  public Response deleteColumn(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String columnName,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      return jaxrsBadRequestError("keyspaceName must be provided").build();
    }
    if (isStringEmpty(tableName)) {
      return jaxrsBadRequestError("table name must be provided").build();
    }
    if (isStringEmpty(columnName)) {
      return jaxrsBadRequestError("columnName must be provided").build();
    }
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
          // !!! TO BE IMPLEMENTED
          return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
        });
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
