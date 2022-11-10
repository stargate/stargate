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
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sgv2TablesResourceImpl extends RestResourceBase implements Sgv2TablesResourceApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(Sgv2TablesResourceImpl.class);

  @Override
  public Uni<RestResponse<Object>> getAllTables(final String keyspaceName, final boolean raw) {
    requireNonEmptyKeyspace(keyspaceName);

    return getTablesAsync(keyspaceName)
        .map(t -> table2table(t, keyspaceName))
        .collect()
        .asList()
        // map to wrapper if needed
        .map(tables -> raw ? tables : new Sgv2RESTResponse<>(tables))
        .map(RestResponse::ok);
  }

  @Override
  public Uni<RestResponse<Object>> getOneTable(
      final String keyspaceName, final String tableName, final boolean raw) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    // NOTE: Can Not use "callWithTable()" as that would return 400 (Bad Request) for
    // missing Table; here we specifically want 404 instead.

    return getTableAsync(keyspaceName, tableName, true)
        .onItem()
        .ifNull()
        .switchTo(
            () ->
                Uni.createFrom()
                    .failure(
                        new WebApplicationException(
                            "Unable to describe table '" + tableName + "'", Status.NOT_FOUND)))
        .map(t -> table2table(t, keyspaceName))
        // map to wrapper if needed
        .map(ks -> raw ? ks : new Sgv2RESTResponse<>(ks))
        .map(RestResponse::ok);
  }

  @Override
  public Uni<RestResponse<Object>> createTable(
      final String keyspaceName, final Sgv2TableAddRequest tableAdd) {
    final String tableName = tableAdd.getName();
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);

    // Need to create name-accessible Map of column objects to make
    // it easier to sort PK columns
    Map<String, Column> columnsByName = new LinkedHashMap<>();
    final Sgv2Table.PrimaryKey primaryKeys = tableAdd.getPrimaryKey();
    final List<Sgv2Table.ClusteringExpression> clusterings = tableAdd.findClusteringExpressions();
    for (Sgv2ColumnDefinition columnDef : tableAdd.getColumnDefinitions()) {
      final String columnName = columnDef.getName();
      ImmutableColumn.Builder column =
          ImmutableColumn.builder().name(columnName).type(columnDef.getTypeDefinition());
      if (primaryKeys.hasPartitionKey(columnName)) {
        column.kind(Column.Kind.PARTITION_KEY);
      } else if (primaryKeys.hasClusteringKey(columnName)) {
        column.kind(Column.Kind.CLUSTERING);
      } else if (columnDef.getIsStatic()) {
        column.kind(Column.Kind.STATIC);
      }
      for (Sgv2Table.ClusteringExpression clustering : clusterings) {
        if (columnName.equals(clustering.getColumn())) {
          if (clustering.hasOrderAsc()) {
            column.order(Column.Order.ASC);
          } else if (clustering.hasOrderDesc()) {
            column.order(Column.Order.DESC);
          } else {
            throw new IllegalArgumentException(
                "Unrecognized ordering value '" + clustering.getOrder() + "'");
          }
        }
      }
      columnsByName.put(columnName, column.build());
    }

    // Columns are all fine, but we must ensure that PK columns are properly sorted
    // so that QueryBuilder will create partition and clustering keys (if any) in
    // correct order, as per "primary key" definition and NOT order of columns passed
    final List<Column> columns = new ArrayList<>(columnsByName.size());
    Stream.concat(primaryKeys.getPartitionKey().stream(), primaryKeys.getClusteringKey().stream())
        .map(key -> columnsByName.remove(key))
        .filter(Objects::nonNull) // should never happen but let QueryBuilder catch, not NPE
        .forEach(column -> columns.add(column));
    // and after PK columns just append remaining columns in the order they were given
    columns.addAll(columnsByName.values());

    QueryOuterClass.Query query =
        new QueryBuilder()
            .create()
            .table(keyspaceName, tableAdd.getName())
            .ifNotExists(tableAdd.getIfNotExists())
            .column(columns)
            .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
            .build();

    return executeQueryAsync(query)
        // No real contents; can ignore ResultSet it seems and only worry about exceptions
        .map(
            any ->
                RestResponse.status(Status.CREATED, Collections.singletonMap("name", tableName)));
  }

  @Override
  public Response updateTable(
      final String keyspaceName, final String tableName, final Sgv2TableAddRequest tableUpdate) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    queryWithTable(
        keyspaceName,
        tableName,
        (tableDef) -> {
          Sgv2Table.TableOptions options = tableUpdate.getTableOptions();
          List<?> clusteringExpressions = options.getClusteringExpression();
          if (clusteringExpressions != null && !clusteringExpressions.isEmpty()) {
            throw new WebApplicationException(
                "Cannot update the clustering order of a table", Status.BAD_REQUEST);
          }
          Integer defaultTTL = options.getDefaultTimeToLive();
          // 09-Dec-2021, tatu: Seems bit odd but this is the way SGv1/RESTv2 checks it,
          //    probably since this is the only thing that can actually be changed:
          if (defaultTTL == null) {
            throw new WebApplicationException(
                "No update provided for defaultTTL", Status.BAD_REQUEST);
          }
          return new QueryBuilder()
              .alter()
              .table(keyspaceName, tableName)
              .withDefaultTTL(options.getDefaultTimeToLive())
              .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
              .build();
        });
    return Response.status(Status.OK).entity(Collections.singletonMap("name", tableName)).build();
  }

  @Override
  public Uni<RestResponse<Object>> deleteTable(final String keyspaceName, final String tableName) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    return executeQueryAsync(
            new QueryBuilder().drop().table(keyspaceName, tableName).ifExists().build())
        .map(any -> RestResponse.noContent());
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for structural conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private Sgv2Table table2table(Schema.CqlTable grpcTable, String keyspace) {
    final List<Sgv2ColumnDefinition> columns = new ArrayList<>();
    final Sgv2Table.PrimaryKey primaryKeys = new Sgv2Table.PrimaryKey();

    // Not very pretty but need to both add columns AND create PrimaryKey defs so:
    for (QueryOuterClass.ColumnSpec column : grpcTable.getPartitionKeyColumnsList()) {
      columns.add(column2column(column, false));
      primaryKeys.addPartitionKey(column.getName());
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getClusteringKeyColumnsList()) {
      columns.add(column2column(column, false));
      primaryKeys.addClusteringKey(column.getName());
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getStaticColumnsList()) {
      columns.add(column2column(column, true));
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getColumnsList()) {
      columns.add(column2column(column, false));
    }
    List<Sgv2Table.ClusteringExpression> clustering =
        clustering2clustering(grpcTable.getClusteringOrdersMap());
    Integer defaultTTL = null;
    String defaultTTLString = grpcTable.getOptionsOrDefault("ttl", null);
    if (defaultTTLString != null) {
      try {
        defaultTTL = Integer.parseInt(defaultTTLString);
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Internal problem: invalid default TTL value '{}' for table '{}:{}', not a valid Integer",
            defaultTTLString,
            keyspace,
            grpcTable.getName());
      }
    }
    final Sgv2Table.TableOptions tableOptions = new Sgv2Table.TableOptions(defaultTTL, clustering);
    return new Sgv2Table(grpcTable.getName(), keyspace, columns, primaryKeys, tableOptions);
  }

  private Sgv2ColumnDefinition column2column(QueryOuterClass.ColumnSpec column, boolean isStatic) {
    return new Sgv2ColumnDefinition(
        column.getName(),
        BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(column.getType(), true),
        isStatic);
  }

  private List<Sgv2Table.ClusteringExpression> clustering2clustering(
      Map<String, Schema.ColumnOrderBy> orders) {
    final List<Sgv2Table.ClusteringExpression> result = new ArrayList<>();
    orders.forEach(
        (k, v) -> {
          switch (v) {
            case ASC:
              result.add(
                  new Sgv2Table.ClusteringExpression(k, Sgv2Table.ClusteringExpression.VALUE_ASC));
              break;
            case DESC:
              result.add(
                  new Sgv2Table.ClusteringExpression(k, Sgv2Table.ClusteringExpression.VALUE_DESC));
              break;
          }
        });
    return result;
  }
}
