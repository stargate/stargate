package io.stargate.sgv2.restapi.service.resources.schemas;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restapi.grpc.BridgeProtoTypeTranslator;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;

public class Sgv2ColumnsResourceImpl extends RestResourceBase implements Sgv2ColumnsResourceApi {
  @Override
  public Uni<RestResponse<Object>> getAllColumns(
      String keyspaceName, String tableName, boolean raw) {
    return getTableAsyncCheckExistence(keyspaceName, tableName, true, Response.Status.BAD_REQUEST)
        .map(t -> table2columns(t))
        // map to wrapper if needed
        .map(t -> raw ? t : new Sgv2RESTResponse<>(t))
        .map(result -> RestResponse.ok(result));
  }

  @Override
  public Uni<RestResponse<Map<String, String>>> createColumn(
      String keyspaceName, String tableName, Sgv2ColumnDefinition columnDefinition) {
    final String columnName = columnDefinition.getName();
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            (tableDef) -> {
              Column.Kind kind =
                  columnDefinition.getIsStatic() ? Column.Kind.STATIC : Column.Kind.REGULAR;
              Column columnDef =
                  ImmutableColumn.builder()
                      .name(columnName)
                      .kind(kind)
                      .type(columnDefinition.getTypeDefinition())
                      .build();
              return new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .addColumn(columnDef)
                  .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                  .build();
            })
        .map(any -> restResponseCreatedWithName(columnName));
  }

  @Override
  public Uni<RestResponse<Object>> getOneColumn(
      String keyspaceName, String tableName, String columnName, boolean raw) {
    return getTableAsyncCheckExistence(keyspaceName, tableName, true, Response.Status.BAD_REQUEST)
        .map(
            tableDef -> {
              Sgv2ColumnDefinition column = findColumn(tableDef, columnName);
              if (column == null) {
                throw new WebApplicationException(
                    String.format("Column '%s' not found in table '%s'", columnName, tableName),
                    Response.Status.NOT_FOUND);
              }
              return column;
            })
        .map(t -> raw ? t : new Sgv2RESTResponse<>(t))
        .map(result -> RestResponse.ok(result));
  }

  @Override
  public Uni<RestResponse<Map<String, String>>> updateColumn(
      String keyspaceName, String tableName, String columnName, Sgv2ColumnDefinition columnUpdate) {
    final String newName = columnUpdate.getName();
    // Avoid call if there is no need to rename
    if (columnName.equals(newName)) {
      return Uni.createFrom().item(restResponseOkWithName(newName));
    }
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            (tableDef) -> {
              // Optional, could let backend verify but this gives us better error reporting
              if (findColumn(tableDef, columnName) == null) {
                throw new WebApplicationException(
                    String.format("Column '%s' not found in table '%s'", columnName, tableName),
                    Response.Status.BAD_REQUEST);
              }
              return new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .renameColumn(columnName, newName)
                  .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                  .build();
            })
        .map(any -> restResponseOkWithName(newName));
  }

  @Override
  public Uni<RestResponse<Void>> deleteColumn(
      String keyspaceName, String tableName, String columnName) {
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            (tableDef) -> {
              // Optional, could let backend verify but this gives us better error reporting
              if (findColumn(tableDef, columnName) == null) {
                throw new WebApplicationException(
                    String.format("Column '%s' not found in table '%s'", columnName, tableName),
                    Response.Status.BAD_REQUEST);
              }
              return new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .dropColumn(columnName)
                  .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
                  .build();
            })
        .map(any -> RestResponse.status(Response.Status.NO_CONTENT));
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
