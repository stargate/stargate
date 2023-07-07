package io.stargate.sgv2.restapi.service.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.RequestParams;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.restapi.config.RestApiConfig;
import io.stargate.sgv2.restapi.grpc.BridgeProtoValueConverters;
import io.stargate.sgv2.restapi.grpc.FromProtoConverter;
import io.stargate.sgv2.restapi.grpc.ToProtoConverter;
import io.stargate.sgv2.restapi.service.models.Sgv2NameResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2RowsResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Base class for resource classes; contains utility/helper methods for which there is no more
 * specific place.
 */
public abstract class RestResourceBase {
  protected static final ObjectMapper JSON_MAPPER = new JsonMapper();
  protected static final ObjectReader MAP_READER = JSON_MAPPER.readerFor(Map.class);
  protected static final int DEFAULT_PAGE_SIZE = 100;

  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE =
          ks ->
              Uni.createFrom()
                  .failure(
                      new WebApplicationException(
                          String.format("Keyspace '%s' not found", ks),
                          Response.Status.BAD_REQUEST));

  private static final Function<String, Uni<? extends QueryOuterClass.Response>>
      MISSING_KEYSPACE_AS_RESPONSE =
          ks ->
              Uni.createFrom()
                  .failure(
                      new WebApplicationException(
                          String.format("Keyspace '%s' not found", ks),
                          Response.Status.BAD_REQUEST));

  protected static final BridgeProtoValueConverters PROTO_CONVERTERS =
      BridgeProtoValueConverters.instance();
  protected static final QueryOuterClass.QueryParameters PARAMETERS_FOR_LOCAL_QUORUM =
      parametersBuilderForLocalQuorum().build();

  @Inject protected SchemaManager schemaManager;

  @Inject protected StargateRequestInfo requestInfo;

  @Inject protected RestApiConfig restApiConfig;

  // // // Helper methods for Schema access

  protected Uni<Schema.CqlKeyspaceDescribe> getKeyspaceAsync(
      String keyspaceName, boolean checkAuthzForKeyspaceMetadata) {
    return checkAuthzForKeyspaceMetadata
        ? schemaManager.getKeyspaceAuthorized(keyspaceName)
        : schemaManager.getKeyspace(keyspaceName);
  }

  protected Multi<Schema.CqlKeyspaceDescribe> getKeyspacesAsync() {
    return schemaManager.getKeyspaces();
  }

  protected Multi<Schema.CqlTable> getTablesAsync(String keyspaceName) {
    return schemaManager.getTables(keyspaceName, MISSING_KEYSPACE);
  }

  protected Uni<Schema.CqlTable> getTableAsync(
      String keyspaceName, String tableName, boolean checkAuthzForTableMetadata) {
    return checkAuthzForTableMetadata
        ? schemaManager.getTableAuthorized(keyspaceName, tableName, MISSING_KEYSPACE)
        : schemaManager.getTable(keyspaceName, tableName, MISSING_KEYSPACE);
  }

  protected Uni<Schema.CqlTable> getTableAsyncCheckExistence(
      String keyspaceName,
      String tableName,
      boolean checkAuthzForTableMetadata,
      Response.Status failCode) {
    return getTableAsync(keyspaceName, tableName, checkAuthzForTableMetadata)
        .onItem()
        .ifNull()
        .switchTo(
            () ->
                Uni.createFrom()
                    .failure(
                        new WebApplicationException(
                            String.format(
                                "Table '%s' not found (in keyspace %s)", tableName, keyspaceName),
                            failCode)));
  }

  // // // Helper methods for Query execution

  /**
   * Gets the metadata of a table (optionally verifying that the access to table metadata is
   * authorized), then uses it to build another CQL query and executes it.
   */
  protected Uni<QueryOuterClass.Response> queryWithTableAsync(
      String keyspaceName,
      String tableName,
      boolean checkAuthzForTableMetadata,
      Function<Schema.CqlTable, QueryOuterClass.Query> queryProducer) {

    Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryProducerUni =
        table -> {
          if (table == null) {
            return Uni.createFrom()
                .failure(
                    new WebApplicationException(
                        String.format(
                            "Table '%s' not found (in keyspace '%s')", tableName, keyspaceName),
                        Response.Status.BAD_REQUEST));
          }
          return Uni.createFrom().item(queryProducer.apply(table));
        };

    if (checkAuthzForTableMetadata) {
      return schemaManager.queryWithSchemaAuthorized(
          keyspaceName, tableName, MISSING_KEYSPACE_AS_RESPONSE, queryProducerUni);
    }
    return schemaManager.queryWithSchema(
        keyspaceName, tableName, MISSING_KEYSPACE_AS_RESPONSE, queryProducerUni);
  }

  protected Uni<QueryOuterClass.Response> executeQueryAsync(QueryOuterClass.Query query) {
    return requestInfo.getStargateBridge().executeQuery(query);
  }

  // // // Helper methods for JSON decoding

  protected static Map<String, Object> parseJsonAsMap(String jsonString) throws IOException {
    return MAP_READER.readValue(jsonString);
  }

  protected static JsonNode parseJsonAsNode(String jsonString) throws IOException {
    return JSON_MAPPER.readTree(jsonString);
  }

  protected static <T> T parseJsonAs(String jsonString, Class<T> cls) throws IOException {
    return JSON_MAPPER.readValue(jsonString, cls);
  }

  // // // Helper methods for Bridge/gRPC query construction

  private static QueryOuterClass.QueryParameters.Builder parametersBuilderForLocalQuorum() {
    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(
            QueryOuterClass.ConsistencyValue.newBuilder()
                .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM))
        .setPageSize(Int32Value.of(DEFAULT_PAGE_SIZE));
  }

  static QueryOuterClass.QueryParameters parametersForPageSizeAndState(
      int pageSizeParam, String pageStateParam) {
    if (isStringEmpty(pageStateParam) && pageSizeParam <= 0) {
      return PARAMETERS_FOR_LOCAL_QUORUM;
    }
    QueryOuterClass.QueryParameters.Builder paramsB = parametersBuilderForLocalQuorum();
    if (!isStringEmpty(pageStateParam)) {
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }
    if (pageSizeParam > 0) {
      paramsB = paramsB.setPageSize(Int32Value.of(pageSizeParam));
    }
    return paramsB.build();
  }

  static QueryOuterClass.QueryParameters parametersForPageSizeStateAndKeyspace(
      int pageSizeParam, String pageStateParam, String keyspace) {
    if (isStringEmpty(pageStateParam) && pageSizeParam <= 0 && isStringEmpty(keyspace)) {
      return PARAMETERS_FOR_LOCAL_QUORUM;
    }
    QueryOuterClass.QueryParameters.Builder paramsB = parametersBuilderForLocalQuorum();
    if (!isStringEmpty(pageStateParam)) {
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }
    if (pageSizeParam > 0) {
      paramsB = paramsB.setPageSize(Int32Value.of(pageSizeParam));
    }
    if (!isStringEmpty(keyspace)) {
      paramsB = paramsB.setKeyspace(StringValue.of(keyspace));
    }
    return paramsB.build();
  }

  // // // Helper methods for Bridge/gRPC type manipulation

  protected static String extractPagingStateFromResultSet(QueryOuterClass.ResultSet rs) {
    BytesValue pagingStateOut = rs.getPagingState();
    if (pagingStateOut.isInitialized()) {
      ByteString rawPS = pagingStateOut.getValue();
      if (!rawPS.isEmpty()) {
        byte[] b = rawPS.toByteArray();
        // Could almost use "ByteBufferUtils.toBase64" but need variant that takes 'byte[]'
        return Base64.getEncoder().encodeToString(b);
      }
    }
    return null;
  }

  protected ToProtoConverter findProtoConverter(
      Schema.CqlTable tableDef, RequestParams requestParams) {
    return PROTO_CONVERTERS.toProtoConverter(tableDef, requestParams);
  }

  public static RestResponse<Object> convertRowsToResponse(
      QueryOuterClass.Response grpcResponse, boolean raw, RequestParams requestParams) {
    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
    final int count = rs.getRowsCount();

    String pageStateStr = extractPagingStateFromResultSet(rs);
    List<Map<String, Object>> rows = convertRows(rs, requestParams);
    Object response = raw ? rows : new Sgv2RowsResponse(count, pageStateStr, rows);
    return RestResponse.ok(response);
  }

  protected static List<Map<String, Object>> convertRows(
      QueryOuterClass.ResultSet rs, RequestParams requestParams) {
    FromProtoConverter converter =
        BridgeProtoValueConverters.instance()
            .fromProtoConverter(rs.getColumnsList(), requestParams);
    List<Map<String, Object>> resultRows = new ArrayList<>();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.mapFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

  protected static ArrayNode convertRowsToArrayNode(
      QueryOuterClass.ResultSet rs, RequestParams requestParams) {
    FromProtoConverter converter =
        BridgeProtoValueConverters.instance()
            .fromProtoConverter(rs.getColumnsList(), requestParams);
    ArrayNode resultRows = JSON_MAPPER.createArrayNode();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.objectNodeFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

  // // // Helper methods for input validation

  protected static final boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }

  protected static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
  }

  // // // Helper methods for JAX-RS response construction

  protected static RestResponse<Sgv2NameResponse> restResponseCreatedWithName(String createdName) {
    return RestResponse.status(Response.Status.CREATED, new Sgv2NameResponse(createdName));
  }

  protected static RestResponse<Sgv2NameResponse> restResponseOkWithName(String name) {
    return RestResponse.status(Response.Status.OK, new Sgv2NameResponse(name));
  }
}
