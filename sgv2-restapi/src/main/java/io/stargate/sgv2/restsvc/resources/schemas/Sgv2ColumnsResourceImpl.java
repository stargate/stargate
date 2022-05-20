package io.stargate.sgv2.restsvc.resources.schemas;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.http.CreateStargateBridgeClient;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoTypeTranslator;
import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateStargateBridgeClient
public class Sgv2ColumnsResourceImpl extends ResourceBase implements Sgv2ColumnsResourceApi {
  @Override
  public Response getAllColumns(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      boolean raw,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    return callWithTable(
        bridge,
        null,
        keyspaceName,
        tableName,
        true,
        (tableDef) -> {
          List<Sgv2ColumnDefinition> columns = table2columns(tableDef);
          final Object payload = raw ? columns : new Sgv2RESTResponse(columns);
          return Response.status(Response.Status.OK).entity(payload).build();
        });
  }

  @Override
  public Response createColumn(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      Sgv2ColumnDefinition columnDefinition,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    final String columnName = columnDefinition.getName();
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("columnName must be provided", Response.Status.BAD_REQUEST);
    }
    return callWithTable(
        bridge,
        null,
        keyspaceName,
        tableName,
        false,
        (tableDef) -> {
          Column.Kind kind =
              columnDefinition.getIsStatic() ? Column.Kind.STATIC : Column.Kind.REGULAR;
          Column columnDef =
              ImmutableColumn.builder()
                  .name(columnName)
                  .kind(kind)
                  .type(columnDefinition.getTypeDefinition())
                  .build();
          QueryOuterClass.Query query =
              new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .addColumn(columnDef)
                  .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                  .build();
          bridge.executeQuery(query);

          return Response.status(Response.Status.CREATED)
              .entity(Collections.singletonMap("name", columnName))
              .build();
        });
  }

  @Override
  public Response getOneColumn(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      String columnName,
      boolean raw,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    if (isStringEmpty(columnName)) {
      throw new WebApplicationException("columnName must be provided", Response.Status.BAD_REQUEST);
    }
    return callWithTable(
        bridge,
        null,
        keyspaceName,
        tableName,
        true,
        (tableDef) -> {
          Sgv2ColumnDefinition column = findColumn(tableDef, columnName);
          if (column == null) {
            throw new WebApplicationException(
                String.format("column '%s' not found in table '%s'", columnName, tableName),
                Response.Status.NOT_FOUND);
          }
          final Object payload = raw ? column : new Sgv2RESTResponse(column);
          return Response.status(Response.Status.OK).entity(payload).build();
        });
  }

  @Override
  public Response updateColumn(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      String columnName,
      Sgv2ColumnDefinition columnUpdate,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    if (isStringEmpty(columnName)) {
      throw new WebApplicationException("columnName must be provided", Response.Status.BAD_REQUEST);
    }
    return callWithTable(
        bridge,
        null,
        keyspaceName,
        tableName,
        false,
        (tableDef) -> {
          final String newName = columnUpdate.getName();
          // Optional, could let backend verify but this gives us better error reporting
          if (findColumn(tableDef, columnName) == null) {
            // 13-Dec-2021, tatu: Seems like maybe it should be NOT_FOUND but SGv1 returns
            // BAD_REQUEST
            throw new WebApplicationException(
                String.format("column '%s' not found in table '%s'", columnName, tableName),
                Response.Status.BAD_REQUEST);
          }
          // Avoid call if there is no need to rename
          if (!columnName.equals(newName)) {
            QueryOuterClass.Query query =
                new QueryBuilder()
                    .alter()
                    .table(keyspaceName, tableName)
                    .renameColumn(columnName, newName)
                    .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                    .build();
            bridge.executeQuery(query);
          }
          return Response.status(Response.Status.OK)
              .entity(Collections.singletonMap("name", newName))
              .build();
        });
  }

  @Override
  public Response deleteColumn(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      String columnName,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    if (isStringEmpty(columnName)) {
      throw new WebApplicationException("columnName must be provided", Response.Status.BAD_REQUEST);
    }
    return callWithTable(
        bridge,
        null,
        keyspaceName,
        tableName,
        false,
        (tableDef) -> {
          // Optional, could let backend verify but this gives us better error reporting
          if (findColumn(tableDef, columnName) == null) {
            throw new WebApplicationException(
                String.format("column '%s' not found in table '%s'", columnName, tableName),
                Response.Status.BAD_REQUEST);
          }
          QueryOuterClass.Query query =
              new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .dropColumn(columnName)
                  .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                  .build();
          bridge.executeQuery(query);
          return Response.status(Response.Status.NO_CONTENT).build();
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
        BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(column.getType(), true),
        isStatic);
  }

  private Sgv2ColumnDefinition findColumn(Schema.CqlTable grpcTable, String columnName) {
    Sgv2ColumnDefinition column =
        findColumn(grpcTable.getPartitionKeyColumnsList(), columnName, false);
    if (column == null) {
      column = findColumn(grpcTable.getClusteringKeyColumnsList(), columnName, false);
      if (column == null) {
        column = findColumn(grpcTable.getStaticColumnsList(), columnName, true);
        if (column == null) {
          column = findColumn(grpcTable.getColumnsList(), columnName, false);
        }
      }
    }
    return column;
  }

  private Sgv2ColumnDefinition findColumn(
      List<QueryOuterClass.ColumnSpec> columns, String columnName, boolean isStatic) {
    for (QueryOuterClass.ColumnSpec column : columns) {
      if (columnName.equals(column.getName())) {
        return column2column(column, isStatic);
      }
    }
    return null;
  }
}
