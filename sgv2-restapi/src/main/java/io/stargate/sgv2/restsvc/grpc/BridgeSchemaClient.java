package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc;
import java.util.List;
import java.util.Objects;

/**
 * Client for accessing Schema information from Bridge/gRPC service. Initially used by REST
 * operations directly, eventually likely hidden behind caching layer.
 */
public class BridgeSchemaClient {
  private final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub;

  protected BridgeSchemaClient(StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub) {
    this.blockingStub = Objects.requireNonNull(blockingStub);
  }

  public static BridgeSchemaClient create(
      StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub) {
    return new BridgeSchemaClient(blockingStub);
  }

  public Schema.CqlTable findTable(String keyspace, String tableName) {
    final Schema.DescribeTableQuery descTableQuery =
        Schema.DescribeTableQuery.newBuilder()
            .setKeyspaceName(keyspace)
            .setTableName(tableName)
            .build();
    // !!! TODO: error handling to expose proper failure downstream
    final Schema.CqlTable table = blockingStub.describeTable(descTableQuery);
    return table;
  }

  public List<Schema.CqlTable> findAllTables(String keyspace) {
    final Schema.DescribeKeyspaceQuery descKsQuery =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspace).build();
    // !!! TODO: error handling to expose proper failure downstream
    final Schema.CqlKeyspaceDescribe ksResponse = blockingStub.describeKeyspace(descKsQuery);
    return ksResponse.getTablesList();
  }
}
