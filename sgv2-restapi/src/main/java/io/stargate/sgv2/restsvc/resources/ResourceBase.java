package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.FromProtoConverter;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2RowsResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Base class for resource classes; contains utility/helper methods for which there is no more
 * specific place.
 */
public abstract class ResourceBase {
  protected static final ObjectMapper JSON_MAPPER = new JsonMapper();
  protected static final ObjectReader MAP_READER = JSON_MAPPER.readerFor(Map.class);
  protected static final int DEFAULT_PAGE_SIZE = 100;

  protected static final BridgeProtoValueConverters PROTO_CONVERTERS =
      BridgeProtoValueConverters.instance();
  protected static final QueryOuterClass.QueryParameters PARAMETERS_FOR_LOCAL_QUORUM =
      parametersBuilderForLocalQuorum().build();

  // // // Helper methods for Schema access

  /**
   * Method to call to try to access Table metadata and call given Function with it (if successful),
   * or, create and return an appropriate error Response if access fails.
   *
   * @param checkIfAuthorized whether to check if the caller is authorized to read this table (not
   *     needed if {@code function} executes a CQL query, see explanations in {@link
   *     StargateBridgeClient#getKeyspaceAsync(String, boolean)}).
   */
  protected Response callWithTable(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      boolean checkIfAuthorized,
      Function<Schema.CqlTable, Response> function) {
    return bridge
        .getTable(keyspaceName, tableName, checkIfAuthorized)
        .map(function)
        .orElseThrow(
            () ->
                new WebApplicationException(
                    String.format("Table '%s' not found (in keyspace %s)", tableName, keyspaceName),
                    Response.Status.BAD_REQUEST));
  }

  // // // Helper methods for JSON decoding

  protected static Map<String, Object> parseJsonAsMap(String jsonString) throws IOException {
    return MAP_READER.readValue(jsonString);
  }

  protected static JsonNode parseJsonAsNode(String jsonString) throws IOException {
    return JSON_MAPPER.readTree(jsonString);
  }

  // // // Helper methods for Bridge/gRPC query construction

  private static QueryOuterClass.QueryParameters.Builder parametersBuilderForLocalQuorum() {
    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(
            QueryOuterClass.ConsistencyValue.newBuilder()
                .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM))
        .setPageSize(Int32Value.of(DEFAULT_PAGE_SIZE));
  }

  protected QueryOuterClass.QueryParameters parametersForPageSizeAndState(
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

  protected ToProtoConverter findProtoConverter(Schema.CqlTable tableDef) {
    return PROTO_CONVERTERS.toProtoConverter(tableDef);
  }

  protected Response fetchRows(
      StargateBridgeClient bridge, QueryOuterClass.Query query, boolean raw) {
    QueryOuterClass.Response grpcResponse = bridge.executeQuery(query);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
    final int count = rs.getRowsCount();

    String pageStateStr = extractPagingStateFromResultSet(rs);
    List<Map<String, Object>> rows = convertRows(rs);
    Object response = raw ? rows : new Sgv2RowsResponse(count, pageStateStr, rows);
    return Response.status(Response.Status.OK).entity(response).build();
  }

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

  protected static ArrayNode convertRowsToArrayNode(QueryOuterClass.ResultSet rs) {
    FromProtoConverter converter =
        BridgeProtoValueConverters.instance().fromProtoConverter(rs.getColumnsList());
    ArrayNode resultRows = JSON_MAPPER.createArrayNode();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.objectNodeFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

  // // // Helper methods for input validation

  protected static void requireNonEmptyKeyspace(String keyspaceName) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException(
          "keyspaceName must be provided", Response.Status.BAD_REQUEST);
    }
  }

  protected static void requireNonEmptyKeyspaceAndTable(String keyspaceName, String tableName) {
    requireNonEmptyKeyspace(keyspaceName);
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("table name must be provided", Response.Status.BAD_REQUEST);
    }
  }

  protected static final boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }

  protected static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
  }

  // // // Helper methods for JAX-RS response construction

  protected static Response restServiceError(Response.Status httpStatus, String failMessage) {
    return Response.status(httpStatus)
        .entity(new RestServiceError(failMessage, httpStatus.getStatusCode()))
        .build();
  }
}
