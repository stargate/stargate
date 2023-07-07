package io.stargate.sgv2.restapi.service.resources;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.config.ImmutableRequestParams;
import io.stargate.sgv2.api.common.cql.builder.*;
import io.stargate.sgv2.restapi.grpc.ToProtoConverter;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.*;
import java.util.stream.Collectors;
import org.jboss.resteasy.reactive.RestResponse;

public class Sgv2RowsResourceImpl extends RestResourceBase implements Sgv2RowsResourceApi {
  @Override
  public Uni<RestResponse<Object>> getRowWithWhere(
      final String keyspaceName,
      final String tableName,
      final String where,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final Boolean compactMap) {
    final List<Column> columns =
        isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw invalidSortParameterException(e);
    }
    final boolean compactMapData = compactMap != null ? compactMap : restApiConfig.compactMapData();
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            false,
            tableDef -> {
              final ToProtoConverter toProtoConverter = findProtoConverter(tableDef, requestParams);

              final List<BuiltCondition> whereConditions;
              try {
                whereConditions = new WhereParser(tableDef, toProtoConverter).parseWhere(where);
              } catch (IllegalArgumentException e) {
                throw invalidWhereParameterException(e);
              }

              if (columns.isEmpty()) {
                return new QueryBuilder()
                    .select()
                    .star()
                    .from(keyspaceName, tableName)
                    .where(whereConditions)
                    .orderBy(sortOrder)
                    .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
                    .build();
              } else {
                return new QueryBuilder()
                    .select()
                    .column(columns)
                    .from(keyspaceName, tableName)
                    .where(whereConditions)
                    .orderBy(sortOrder)
                    .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
                    .build();
              }
            })
        .map(response -> convertRowsToResponse(response, raw, requestParams));
  }

  @Override
  public Uni<RestResponse<Object>> getRows(
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final Boolean compactMap) {
    final List<Column> columns =
        isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw invalidSortParameterException(e);
    }
    final boolean compactMapData =
        (compactMap != null) ? compactMap : restApiConfig.compactMapData();
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            false,
            tableDef -> {
              final ToProtoConverter toProtoConverter = findProtoConverter(tableDef, requestParams);
              try {
                return buildGetRowsByPKQuery(
                    keyspaceName,
                    tableName,
                    path,
                    columns,
                    sortOrder,
                    tableDef,
                    pageSizeParam,
                    pageStateParam,
                    toProtoConverter);
              } catch (IllegalArgumentException e) {
                throw new WebApplicationException(
                    "Invalid path for row to find, problem: " + e.getMessage(), Status.BAD_REQUEST);
              }
            })
        .map(response -> convertRowsToResponse(response, raw, requestParams));
  }

  @Override
  public Uni<RestResponse<Object>> getAllRows(
      final String keyspaceName,
      final String tableName,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final Boolean compactMap) {
    final boolean compactMapData =
        (compactMap != null) ? compactMap : restApiConfig.compactMapData();
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    List<Column> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw invalidSortParameterException(e);
    }

    // 15-Nov-2022, tatu: Since we do not need Table definition for anything let's just
    //     call simpler method. Note that this may mean we get slightly different error
    //     for missing Table than with "getRowWithWhere()" and "getRows()"
    final QueryOuterClass.Query query =
        columns.isEmpty()
            ? new QueryBuilder()
                .select()
                .star()
                .from(keyspaceName, tableName)
                .orderBy(sortOrder)
                .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
                .build()
            : new QueryBuilder()
                .select()
                .column(columns)
                .from(keyspaceName, tableName)
                .orderBy(sortOrder)
                .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
                .build();
    return executeQueryAsync(query)
        .map(response -> convertRowsToResponse(response, raw, requestParams));
  }

  @Override
  public Uni<RestResponse<Object>> createRow(
      final String keyspaceName,
      final String tableName,
      final String payloadAsString,
      final Boolean compactMap) {
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      throw invalidPayloadException(e);
    }
    final boolean compactMapData =
        (compactMap != null) ? compactMap : restApiConfig.compactMapData();
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            false,
            tableDef -> {
              final ToProtoConverter toProtoConverter = findProtoConverter(tableDef, requestParams);

              try {
                return buildAddRowQuery(keyspaceName, tableName, payloadMap, toProtoConverter);
              } catch (IllegalArgumentException e) {
                throw new WebApplicationException(
                    "Invalid path for row to create, problem: " + e.getMessage(),
                    Status.BAD_REQUEST);
              }
            })
        .map(any -> RestResponse.status(Response.Status.CREATED, payloadAsString));
  }

  @Override
  public Uni<RestResponse<Object>> updateRows(
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final Boolean compactMap) {
    return modifyRow(
        keyspaceName,
        tableName,
        path,
        raw,
        payload,
        (compactMap != null) ? compactMap : restApiConfig.compactMapData());
  }

  @Override
  public Uni<RestResponse<Object>> patchRows(
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final Boolean compactMap) {
    return modifyRow(
        keyspaceName,
        tableName,
        path,
        raw,
        payload,
        (compactMap != null) ? compactMap : restApiConfig.compactMapData());
  }

  @Override
  public Uni<RestResponse<Object>> deleteRows(
      final String keyspaceName, final String tableName, final List<PathSegment> path) {
    // Map data in the primary key is not supported/tested via REST API
    // Since this flag is used only for map data in the primary key, this is not supported in this
    // API
    // since the converters require this flag, we set it to true here.
    final boolean compactMapData = true;
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            false,
            (tableDef) -> {
              final ToProtoConverter toProtoConverter = findProtoConverter(tableDef, requestParams);
              try {
                return buildDeleteRowsByPKCQuery(
                    keyspaceName, tableName, path, tableDef, toProtoConverter);
              } catch (IllegalArgumentException e) {
                throw new WebApplicationException(
                    "Invalid path for row to delete, problem: " + e.getMessage(),
                    Status.BAD_REQUEST);
              }
            })
        .map(any -> RestResponse.status(Response.Status.NO_CONTENT));
  }

  /** Implementation of POST/PATCH (update/patch rows) endpoints */
  private Uni<RestResponse<Object>> modifyRow(
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payloadAsString,
      final boolean compactMapData) {
    ImmutableRequestParams requestParams =
        ImmutableRequestParams.builder().compactMapData(compactMapData).build();
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      throw invalidPayloadException(e);
    }
    return queryWithTableAsync(
            keyspaceName,
            tableName,
            false,
            tableDef -> {
              final ToProtoConverter toProtoConverter = findProtoConverter(tableDef, requestParams);
              try {
                return buildUpdateRowQuery(
                    keyspaceName, tableName, path, tableDef, payloadMap, toProtoConverter);
              } catch (IllegalArgumentException e) {
                throw new WebApplicationException(
                    "Invalid path for row to update, problem: " + e.getMessage(),
                    Status.BAD_REQUEST);
              }
            })
        // apparently no useful data in ResultSet, we should simply return payload we got:
        .map(any -> raw ? payloadMap : new Sgv2RESTResponse<>(payloadMap))
        .map(payload -> RestResponse.ok(payload));
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Query construction
  /////////////////////////////////////////////////////////////////////////
   */

  protected QueryOuterClass.Query buildGetRowsByPKQuery(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      List<Column> columns,
      Map<String, Column.Order> sortOrder,
      Schema.CqlTable tableDef,
      int pageSizeParam,
      String pageStateParam,
      ToProtoConverter toProtoConverter) {
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);
    List<BuiltCondition> whereConditions = new ArrayList<>(keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      QueryOuterClass.Value value = toProtoConverter.protoValueFromStringified(fieldName, keyValue);
      whereConditions.add(BuiltCondition.of(fieldName, Predicate.EQ, value));
    }

    if (columns.isEmpty()) {
      return new QueryBuilder()
          .select()
          .star()
          .from(keyspaceName, tableName)
          .where(whereConditions)
          .orderBy(sortOrder)
          .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
          .build();
    }
    return new QueryBuilder()
        .select()
        .column(columns)
        .from(keyspaceName, tableName)
        .where(whereConditions)
        .orderBy(sortOrder)
        .parameters(parametersForPageSizeAndState(pageSizeParam, pageStateParam))
        .build();
  }

  private QueryOuterClass.Query buildDeleteRowsByPKCQuery(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      Schema.CqlTable tableDef,
      ToProtoConverter toProtoConverter) {
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);

    List<BuiltCondition> whereConditions = new ArrayList<>(keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      QueryOuterClass.Value value = toProtoConverter.protoValueFromStringified(fieldName, keyValue);
      whereConditions.add(BuiltCondition.of(fieldName, Predicate.EQ, value));
    }

    return new QueryBuilder()
        .delete()
        .from(keyspaceName, tableName)
        .where(whereConditions)
        .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
        .build();
  }

  private List<QueryOuterClass.ColumnSpec> getAndValidatePrimaryKeys(
      Schema.CqlTable tableDef, int keysIncluded) {
    List<QueryOuterClass.ColumnSpec> partitionKeys = tableDef.getPartitionKeyColumnsList();

    // Check we have "just right" number of keys
    if (keysIncluded < partitionKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%d) less than number of partition keys (%d)",
              keysIncluded, partitionKeys.size()));
    }
    List<QueryOuterClass.ColumnSpec> clusteringKeys = tableDef.getClusteringKeyColumnsList();
    List<QueryOuterClass.ColumnSpec> primaryKeys = concat(partitionKeys, clusteringKeys);
    if (keysIncluded > primaryKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%d) exceeds number of partition keys (%d) and clustering keys (%d)",
              keysIncluded, partitionKeys.size(), clusteringKeys.size()));
    }
    return primaryKeys;
  }

  protected QueryOuterClass.Query buildAddRowQuery(
      String keyspaceName,
      String tableName,
      Map<String, Object> payloadMap,
      ToProtoConverter toProtoConverter) {
    List<ValueModifier> valueModifiers = new ArrayList<>(payloadMap.size());
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String columnName = entry.getKey();
      QueryOuterClass.Value value =
          toProtoConverter.protoValueFromLooselyTyped(columnName, entry.getValue());
      valueModifiers.add(ValueModifier.set(columnName, value));
    }
    return new QueryBuilder()
        .insertInto(keyspaceName, tableName)
        .value(valueModifiers)
        .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
        .build();
  }

  protected QueryOuterClass.Query buildUpdateRowQuery(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      Schema.CqlTable tableDef,
      Map<String, Object> payloadMap,
      ToProtoConverter toProtoConverter) {

    Set<String> counters = findCounterNames(tableDef);

    // First, values to update
    List<ValueModifier> valueModifiers = new ArrayList<>(payloadMap.size());
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String columnName = entry.getKey();
      QueryOuterClass.Value value =
          toProtoConverter.protoValueFromLooselyTyped(columnName, entry.getValue());
      // Special handling for counter updates, cannot Set
      ValueModifier mod =
          counters.contains(columnName)
              ? ValueModifier.of(
                  ValueModifier.Target.column(columnName),
                  ValueModifier.Operation.INCREMENT,
                  Term.of(value))
              : ValueModifier.set(columnName, value);
      valueModifiers.add(mod);
    }

    // Second, where clause from primary key
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);
    List<BuiltCondition> whereConditions = new ArrayList<>(keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String columnName = column.getName();
      QueryOuterClass.Value value =
          toProtoConverter.protoValueFromStringified(columnName, keyValue);
      whereConditions.add(BuiltCondition.of(columnName, Predicate.EQ, value));
    }

    return new QueryBuilder()
        .update(keyspaceName, tableName)
        .value(valueModifiers)
        .where(whereConditions)
        .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
        .build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Structural/nested/scalar conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private Map<String, Column.Order> decodeSortOrder(String sortOrderJson) {
    if (isStringEmpty(sortOrderJson)) {
      return Collections.emptyMap();
    }
    JsonNode root;
    try {
      root = parseJsonAsNode(sortOrderJson);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid 'sort' argument, not valid JSON (%s): %s", sortOrderJson, e.getMessage()));
    }
    if (!root.isObject()) {
      throw new IllegalArgumentException(
          String.format("Invalid 'sort' argument, not JSON Object (%s)", sortOrderJson));
    }
    if (root.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Column.Order> order = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> it = root.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      String value = entry.getValue().asText();
      order.put(entry.getKey(), "desc".equals(value) ? Column.Order.DESC : Column.Order.ASC);
    }
    return order;
  }

  private Set<String> findCounterNames(Schema.CqlTable tableDef) {
    // Only need to check static and regular columns; primary keys cannot be Counters.
    // NOTE: could probably optimize knowing that Counters are "all-or-nothing", cannot
    // mix-and-match. But for now will just check them all.

    // also don't think Counter is valid for static columns?
    Set<String> counterNames = findCounterNames(null, tableDef.getColumnsList());
    counterNames = findCounterNames(counterNames, tableDef.getColumnsList());
    return (counterNames == null) ? Collections.emptySet() : counterNames;
  }

  private Set<String> findCounterNames(
      Set<String> counterNames, List<QueryOuterClass.ColumnSpec> columns) {
    for (QueryOuterClass.ColumnSpec column : columns) {
      if (column.getType().getBasic() == QueryOuterClass.TypeSpec.Basic.COUNTER) {
        if (counterNames == null) {
          counterNames = new HashSet<>();
        }
        counterNames.add(column.getName());
      }
    }
    return counterNames;
  }

  private List<Column> splitColumns(String columnStr) {
    return Arrays.stream(columnStr.split(","))
        .map(String::trim)
        .filter(c -> c.length() != 0)
        .map(c -> Column.reference(c))
        .collect(Collectors.toList());
  }

  private static <T> List<T> concat(List<T> a, List<T> b) {
    if (a.isEmpty()) {
      return b;
    }
    if (b.isEmpty()) {
      return a;
    }
    ArrayList<T> result = new ArrayList<>(a);
    result.addAll(b);
    return result;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for error reporting
  /////////////////////////////////////////////////////////////////////////
   */

  private WebApplicationException invalidPayloadException(Exception e) {
    throw new WebApplicationException(
        "Invalid JSON payload: " + e.getMessage(), Status.BAD_REQUEST);
  }

  private WebApplicationException invalidSortParameterException(IllegalArgumentException e) {
    return new WebApplicationException(
        "Invalid 'sort' parameter, problem: " + e.getMessage(), Status.BAD_REQUEST);
  }

  private WebApplicationException invalidWhereParameterException(IllegalArgumentException e) {
    return new WebApplicationException(
        "Invalid 'where' parameter, problem: " + e.getMessage(), Status.BAD_REQUEST);
  }
}
