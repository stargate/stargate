package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.grpc.StatusRuntimeException;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
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

  // // // Helper methods for Schema access

  /**
   * Method to call to try to access Table metadata and call given Function with it (if successful),
   * or, create and return an appropriate error Response if access fails.
   */
  protected Response callWithTable(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      Function<Schema.CqlTable, Response> function) {
    final Schema.CqlTable tableDef;
    try {
      tableDef = BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    } catch (StatusRuntimeException grpcE) {
      switch (grpcE.getStatus().getCode()) {
        case NOT_FOUND:
          final String msg = grpcE.getMessage();
          if (msg.contains("Keyspace not found")) {
            return jaxrsBadRequestError(String.format("Keyspace '%s' not found", keyspaceName))
                .build();
          }
          return jaxrsBadRequestError(
                  String.format("Table '%s' not found (in keyspace %s)", tableName, keyspaceName))
              .build();
      }
      throw grpcE;
    }
    return function.apply(tableDef);
  }

  // // // Helper methods for Response construction

  protected static Response.ResponseBuilder jaxrsResponse(Response.Status status) {
    return Response.status(status);
  }

  protected static Response.ResponseBuilder jaxrsServiceError(Response.Status status, String msg) {
    return Response.status(status).entity(new RestServiceError(msg, status.getStatusCode()));
  }

  protected static Response.ResponseBuilder jaxrsBadRequestError(String msgSuffix) {
    Response.Status status = Response.Status.BAD_REQUEST;
    String msg = "Bad request: " + msgSuffix;
    return Response.status(status).entity(new RestServiceError(msg, status.getStatusCode()));
  }

  // // // Helper methods for JSON decoding

  protected static Map<String, Object> parseJsonAsMap(String jsonString) throws IOException {
    return MAP_READER.readValue(jsonString);
  }

  protected static JsonNode parseJsonAsNode(String jsonString) throws IOException {
    return JSON_MAPPER.readTree(jsonString);
  }

  // // // Helper methods for Bridge/gRPC query construction

  protected static QueryOuterClass.QueryParameters parametersForLocalQuorum() {
    return parametersBuilderForLocalQuorum().build();
  }

  protected static QueryOuterClass.QueryParameters.Builder parametersBuilderForLocalQuorum() {
    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(
            QueryOuterClass.ConsistencyValue.newBuilder()
                .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM));
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

  // // // Helper methods for input validation

  /**
   * Method that checks for some common types of invalidity for Auth Token: currently simply its
   * existence (cannot be {@code null}) or empty ({code !string.isEmpty()}), but may be extended
   * with heuristics in future.
   */
  protected static final boolean isAuthTokenInvalid(String authToken) {
    return isStringEmpty(authToken);
  }

  protected static final boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }

  protected Response fetchRows(
      StargateGrpc.StargateBlockingStub blockingStub,
      int pageSizeParam,
      String pageStateParam,
      boolean raw,
      String cql,
      QueryOuterClass.Values.Builder values) {
    QueryOuterClass.QueryParameters.Builder paramsB = parametersBuilderForLocalQuorum();
    if (!isStringEmpty(pageStateParam)) {
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }
    int pageSize = DEFAULT_PAGE_SIZE;
    if (pageSizeParam > 0) {
      pageSize = pageSizeParam;
    }
    paramsB = paramsB.setPageSize(Int32Value.of(pageSize));

    QueryOuterClass.Query.Builder b =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB).setCql(cql);
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

  private static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
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
}
