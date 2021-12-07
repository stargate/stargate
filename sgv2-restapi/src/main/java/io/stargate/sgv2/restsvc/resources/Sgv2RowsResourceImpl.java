package io.stargate.sgv2.restsvc.resources;

import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.grpc.FromProtoConverter;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.Sgv2RowsResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// note: JAX-RS Class Annotations MUST be in the impl class; only method annotations inherited
// (but Swagger allows inheritance)
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2RowsResourceImpl extends ResourceBase implements Sgv2RowsResourceApi {
  protected static final int DEFAULT_PAGE_SIZE = 100;

  // Singleton resource so no need to be static
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  /** Entity used to connect to backend gRPC service. */
  @Inject protected GrpcClientFactory grpcFactory;

  /*
  /////////////////////////////////////////////////////////////////////////
  // REST API endpoint implementation methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Override
  public Response getRowWithWhere(
      final String token,
      final String keyspaceName,
      final String tableName,
      final String where,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sort,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    // !!! TO BE IMPLEMENTED
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response getRows(
      final String token,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sort,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    List<String> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));

    // To bind path/key parameters, need converter; and for that we need table metadata:
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    final String cql =
        buildGetRowsByPKCQL(
            keyspaceName, tableName, path, columns, tableDef, valuesBuilder, toProtoConverter);
    logger.info("getRows(): try to call backend with CQL of '{}'", cql);

    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, valuesBuilder);
  }

  @Override
  public javax.ws.rs.core.Response getAllRows(
      final String token,
      final String keyspaceName,
      final String tableName,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final String sort,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    List<String> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final String cql = buildGetAllRowsCQL(keyspaceName, tableName, columns);
    logger.info("getAllRows(): try to call backend with CQL of '{}'", cql);

    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, null);
  }

  @Override
  public Response createRow(
      final String token,
      final String keyspaceName,
      final String tableName,
      final String payloadAsString,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      return jaxrsServiceError(
              Response.Status.BAD_REQUEST, "Invalid JSON payload: " + e.getMessage())
          .build();
    }
    final String cql;
    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));

    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    try {
      cql = buildAddRowCQL(keyspaceName, tableName, payloadMap, valuesBuilder, toProtoConverter);
    } catch (IllegalArgumentException e) {
      // No logging since this is due to something bad client sent:
      return jaxrsBadRequestError(e.getMessage()).build();
    } catch (Exception e) {
      // Unrecognized problem to be converted to something known; log:
      logger.error("Unknown payload bind problem: " + e.getMessage(), e);
      return jaxrsServiceError(
              Response.Status.INTERNAL_SERVER_ERROR, "Failure to bind payload: " + e.getMessage())
          .build();
    }
    logger.info("createRow(): try to call backend with CQL of '{}'", cql);

    QueryOuterClass.QueryParameters params = parametersForLocalQuorum();
    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(params)
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);
          // apparently no useful data in ResultSet, we should simply return payload we got:
          return jaxrsResponse(Response.Status.CREATED).entity(payloadAsString).build();
        });
  }

  @Override
  public Response updateRows(
      final String token,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    // !!! TO BE IMPLEMENTED
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response deleteRows(
      final String token,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    // To bind path/key parameters, need converter; and for that we need table metadata:
    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    final String cql =
        buildDeleteRowsByPKCQL(
            keyspaceName, tableName, path, tableDef, valuesBuilder, toProtoConverter);

    QueryOuterClass.QueryParameters params = parametersForLocalQuorum();
    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(params)
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
          return jaxrsResponse(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response patchRows(
      final String token,
      final String keyspaceName,
      final String tableName,
      final List<PathSegment> path,
      final boolean raw,
      final String payload,
      final HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    // !!! TO BE IMPLEMENTED
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for row access
  /////////////////////////////////////////////////////////////////////////
   */

  protected javax.ws.rs.core.Response fetchRows(
      StargateGrpc.StargateBlockingStub blockingStub,
      int pageSizeParam,
      String pageStateParam,
      boolean raw,
      String cql,
      QueryOuterClass.Values.Builder values) {
    QueryOuterClass.QueryParameters.Builder paramsB = parametersBuilderForLocalQuorum();
    if (!isStringEmpty(pageStateParam)) {
      // surely there must better way to make Protobuf accept plain old byte[]? But if not:
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }
    int pageSize = DEFAULT_PAGE_SIZE;
    if (pageSizeParam > 0) {
      pageSize = pageSizeParam;
    }
    paramsB = paramsB.setPageSize(Int32Value.of(pageSize));

    QueryOuterClass.Query.Builder b =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql);
    if (values != null) {
      b = b.setValues(values);
    }
    final QueryOuterClass.Query query = b.build();
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
          final int count = rs.getRowsCount();

          String pageStateStr = extractPagingStateFromResultSet(rs);
          List<Map<String, Object>> rows = convertRows(rs);
          Object response = raw ? rows : new Sgv2RowsResponse(count, pageStateStr, rows);
          return jaxrsResponse(Response.Status.OK).entity(response).build();
        });
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
      List<String> columns,
      Schema.CqlTable tableDef,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    SelectFrom selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName);
    Select select;
    if (columns.isEmpty()) {
      select = selectFrom.all();
    } else {
      select = selectFrom.columns(columns);
    }
    final int keysIncluded = pkValues.size();
    final List<QueryOuterClass.ColumnSpec> primaryKeys =
        getAndValidatePrimaryKeys(tableDef, keysIncluded);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      select = select.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker());
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(fieldName, keyValue));
    }

    return select.asCql();
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
    OngoingWhereClause<Delete> delete = QueryBuilder.deleteFrom(keyspaceName, tableName);
    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      delete = delete.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker());
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(fieldName, keyValue));
    }

    return ((BuildableQuery) delete).asCql();
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

  protected String buildGetAllRowsCQL(String keyspaceName, String tableName, List<String> columns) {
    // return String.format("SELECT %s from %s.%s", fields, keyspaceName, tableName);
    SelectFrom selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName);
    if (columns.isEmpty()) {
      return selectFrom.all().asCql();
    }
    return selectFrom.columns(columns).asCql();
  }

  protected String buildAddRowCQL(
      String keyspaceName,
      String tableName,
      Map<String, Object> payloadMap,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    OngoingValues insert = QueryBuilder.insertInto(keyspaceName, tableName);
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String fieldName = entry.getKey();
      insert = insert.value(fieldName, QueryBuilder.bindMarker());
      valuesBuilder.addValues(
          toProtoConverter.protoValueFromLooselyTyped(fieldName, entry.getValue()));
    }
    return ((Insert) insert).asCql();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Structural/nested/scalar conversions
  /////////////////////////////////////////////////////////////////////////
   */

  protected List<Map<String, Object>> convertRows(QueryOuterClass.ResultSet rs) {
    FromProtoConverter converter =
        BridgeProtoValueConverters.instance().fromProtoConverter(rs.getColumnsList());
    List<Map<String, Object>> resultRows = new ArrayList<>();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.mapFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

  protected static List<String> splitColumns(String columnStr) {
    return Arrays.stream(columnStr.split(","))
        .map(String::trim)
        .filter(c -> c.length() != 0)
        .collect(Collectors.toList());
  }

  protected static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
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
