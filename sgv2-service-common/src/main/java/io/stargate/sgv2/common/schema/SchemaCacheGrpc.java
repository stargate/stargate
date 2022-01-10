/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.common.schema;

import com.google.protobuf.BytesValue;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.GetSchemaNotificationsParams;
import io.stargate.proto.StargateGrpc.StargateStub;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.SingleStreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Helper class to keep gRPC request boilerplate code out of {@link SchemaCache}. */
class SchemaCacheGrpc {

  static void registerChangeObserver(
      StargateStub stub, StreamObserver<Schema.SchemaNotification> observer) {
    stub.getSchemaNotifications(GetSchemaNotificationsParams.newBuilder().build(), observer);
  }

  static CompletionStage<Map<String, CqlKeyspaceDescribe>> getAllKeyspaces(StargateStub stub) {
    return SchemaCacheGrpc.getKeyspaceNames(stub)
        .thenCompose(
            keyspaceNames -> {
              List<CompletionStage<CqlKeyspaceDescribe>> keyspaceFutures =
                  new ArrayList<>(keyspaceNames.size());
              for (String keyspaceName : keyspaceNames) {
                keyspaceFutures.add(SchemaCacheGrpc.describeKeyspace(stub, keyspaceName));
              }
              return Futures.sequence(keyspaceFutures)
                  .thenApply(
                      keyspaces ->
                          keyspaces.stream()
                              .collect(
                                  Collectors.toMap(
                                      desc -> desc.getCqlKeyspace().getName(),
                                      Function.identity())));
            });
  }

  private static CompletionStage<List<String>> getKeyspaceNames(StargateStub stub) {
    List<String> keyspaceNames = new ArrayList<>();
    return getKeyspaceNames(stub, keyspaceNames, null);
  }

  private static CompletionStage<List<String>> getKeyspaceNames(
      StargateStub stub, List<String> keyspaceNames, BytesValue pagingState) {
    Query.Builder query =
        Query.newBuilder().setCql("SELECT keyspace_name FROM system_schema.keyspaces");
    if (pagingState != null) {
      query.setParameters(QueryParameters.newBuilder().setPagingState(pagingState).build());
    }
    CompletionStage<Response> nextPageFuture =
        SingleStreamObserver.toFuture(observer -> stub.executeQuery(query.build(), observer));
    return nextPageFuture.thenCompose(
        response -> {
          ResultSet resultSet = response.getResultSet();
          resultSet
              .getRowsList()
              .forEach(row -> keyspaceNames.add(Values.string(row.getValues(0))));
          if (resultSet.hasPagingState()) {
            BytesValue nextPagingState = resultSet.getPagingState();
            return getKeyspaceNames(stub, keyspaceNames, nextPagingState);
          } else {
            return CompletableFuture.completedFuture(keyspaceNames);
          }
        });
  }

  private static CompletionStage<CqlKeyspaceDescribe> describeKeyspace(
      StargateStub stub, String keyspaceName) {
    return SingleStreamObserver.toFuture(
        observer ->
            stub.describeKeyspace(
                DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build(),
                observer));
  }
}
