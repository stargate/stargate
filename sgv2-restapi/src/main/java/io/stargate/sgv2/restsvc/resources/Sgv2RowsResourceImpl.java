package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Value;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// note: JAX-RS Class Annotations MUST be in the impl class; only method annotations inherited
// (but Swagger allows inheritance)
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2RowsResourceImpl extends ResourceBase implements Sgv2RowsResourceApi {

  // Singleton resource so no need to be static
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  /*
  /////////////////////////////////////////////////////////////////////////
  // REST API endpoint implementation methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Override
  public Response getRowWithWhere(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final String where,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    if (isStringEmpty(where)) {
      throw new WebApplicationException("where parameter is required", Status.BAD_REQUEST);
    }

    final List<Column> columns =
        isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }

    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);

    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();
    List<BuiltCondition> whereConditions =
        new WhereParser(tableDef, toProtoConverter).parseWhere(where, valuesBuilder);

    String cql;
    if (columns.isEmpty()) {
      cql =
          new QueryBuilder()
              .select()
              .star()
              .from(keyspaceName, tableName)
              .where(whereConditions)
              .orderBy(sortOrder)
              .build();
    } else {
      cql =
          new QueryBuilder()
              .select()
              .column(columns)
              .from(keyspaceName, tableName)
              .where(whereConditions)
              .orderBy(sortOrder)
              .build();
    }
    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, valuesBuilder);
  }

  @Override
  public Response getRows(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    final List<Column> columns =
        isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }

    // To bind path/key parameters, need converter; and for that we need table metadata:
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    final String cql =
        buildGetRowsByPKCQL(
            keyspaceName,
            tableName,
            path,
            columns,
            sortOrder,
            tableDef,
            valuesBuilder,
            toProtoConverter);
    logger.info("getRows(): try to call backend with CQL of '{}'", cql);

    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, valuesBuilder);
  }

  @Override
  public javax.ws.rs.core.Response getAllRows(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sortJson,
      final HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    List<Column> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    Map<String, Column.Order> sortOrder;
    try {
      sortOrder = decodeSortOrder(sortJson);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }
    final String cql;
    if (columns.isEmpty()) {
      cql =
          new QueryBuilder()
              .select()
              .star()
              .from(keyspaceName, tableName)
              .orderBy(sortOrder)
              .build();
    } else {
      cql =
          new QueryBuilder()
              .select()
              .column(columns)
              .from(keyspaceName, tableName)
              .orderBy(sortOrder)
              .build();
    }
    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, null);
  }

  @Override
  public Response createRow(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final String payloadAsString,
      final HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      throw new WebApplicationException(
          "Invalid JSON payload: " + e.getMessage(), Status.BAD_REQUEST);
    }
    final String cql;

    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    try {
      cql = buildAddRowCQL(keyspaceName, tableName, payloadMap, valuesBuilder, toProtoConverter);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }
    logger.info("createRow(): try to call backend with CQL of '{}'", cql);

    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(parametersForLocalQuorum())
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);
    // apparently no useful data in ResultSet, we should simply return payload we got:
    return Response.status(Status.CREATED).entity(payloadAsString).build();
  }

  @Override
  public Response updateRows(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final HttpServletRequest request) {
    return modifyRow(blockingStub, keyspaceName, tableName, path, raw, payload, request);
  }

  @Override
  public Response deleteRows(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    // To bind path/key parameters, need converter; and for that we need table metadata:
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    final String cql =
        buildDeleteRowsByPKCQL(
            keyspaceName, tableName, path, tableDef, valuesBuilder, toProtoConverter);

    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(parametersForLocalQuorum())
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    return Response.status(Status.NO_CONTENT).build();
  }

  @Override
  public Response patchRows(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final HttpServletRequest request) {
    return modifyRow(blockingStub, keyspaceName, tableName, path, raw, payload, request);
  }

  /** Implementation of POST/PATCH (update/patch rows) endpoints */
  private Response modifyRow(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payloadAsString,
      final HttpServletRequest request) {
    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      throw new WebApplicationException(
          "Invalid JSON payload: " + e.getMessage(), Status.BAD_REQUEST);
    }
    final String cql;
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    try {
      cql =
          buildUpdateRowCQL(
              keyspaceName, tableName, path, tableDef, payloadMap, valuesBuilder, toProtoConverter);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }
    logger.info("modifyRow(): try to call backend with CQL of '{}'", cql);

    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(parametersForLocalQuorum())
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);
    // apparently no useful data in ResultSet, we should simply return payload we got:
    final Object responsePayload = raw ? payloadMap : new Sgv2RESTResponse(payloadMap);
    return Response.status(Status.OK).entity(responsePayload).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Query construction
  /////////////////////////////////////////////////////////////////////////
   */

  protected String buildGetRowsByPKCQL(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      List<Column> columns,
      Map<String, Column.Order> sortOrder,
      Schema.CqlTable tableDef,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);
    List<BuiltCondition> whereConditions = new ArrayList<>(keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      whereConditions.add(BuiltCondition.ofMarker(fieldName, Predicate.EQ));
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(fieldName, keyValue));
    }

    if (columns.isEmpty()) {
      return new QueryBuilder()
          .select()
          .star()
          .from(keyspaceName, tableName)
          .where(whereConditions)
          .orderBy(sortOrder)
          .build();
    }
    return new QueryBuilder()
        .select()
        .column(columns)
        .from(keyspaceName, tableName)
        .where(whereConditions)
        .orderBy(sortOrder)
        .build();
  }

  private String buildDeleteRowsByPKCQL(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      Schema.CqlTable tableDef,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);

    List<BuiltCondition> whereConditions = new ArrayList<>(keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      whereConditions.add(BuiltCondition.ofMarker(fieldName, Predicate.EQ));
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(fieldName, keyValue));
    }

    return new QueryBuilder().delete().from(keyspaceName, tableName).where(whereConditions).build();
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

  protected String buildAddRowCQL(
      String keyspaceName,
      String tableName,
      Map<String, Object> payloadMap,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    List<ValueModifier> valueModifiers = new ArrayList<>(payloadMap.size());
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String columnName = entry.getKey();
      valueModifiers.add(ValueModifier.marker(columnName));
      valuesBuilder.addValues(
          toProtoConverter.protoValueFromLooselyTyped(columnName, entry.getValue()));
    }
    return new QueryBuilder().insertInto(keyspaceName, tableName).value(valueModifiers).build();
  }

  protected String buildUpdateRowCQL(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      Schema.CqlTable tableDef,
      Map<String, Object> payloadMap,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {

    // Need to process values in order they are included in query...

    Set<String> counters = findCounterNames(tableDef);

    // First, values to update
    List<ValueModifier> valueModifiers = new ArrayList<>(payloadMap.size());
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String columnName = entry.getKey();
      // Special handling for counter updates, cannot Set
      ValueModifier mod =
          counters.contains(columnName)
              ? ValueModifier.of(
                  ValueModifier.Target.column(columnName),
                  ValueModifier.Operation.INCREMENT,
                  Value.marker())
              : ValueModifier.marker(columnName);
      valueModifiers.add(mod);
      valuesBuilder.addValues(
          toProtoConverter.protoValueFromLooselyTyped(columnName, entry.getValue()));
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
      whereConditions.add(BuiltCondition.ofMarker(columnName, Predicate.EQ));
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(columnName, keyValue));
    }

    return new QueryBuilder()
        .update(keyspaceName, tableName)
        .value(valueModifiers)
        .where(whereConditions)
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
}
