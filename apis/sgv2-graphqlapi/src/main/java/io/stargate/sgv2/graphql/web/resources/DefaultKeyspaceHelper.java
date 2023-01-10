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
package io.stargate.sgv2.graphql.web.resources;

import com.google.protobuf.BytesValue;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the oldest, non-system keyspace in the database. It is used as a default when no keyspace
 * name is provided in the URL path.
 *
 * @see DmlResource#get(String, String, String)
 * @see DmlResource#postJson(GraphqlJsonBody, String)
 * @see DmlResource#postGraphql(String, String)
 */
class DefaultKeyspaceHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultKeyspaceHelper.class);
  private static final Query SELECT_KEYSPACE_NAMES =
      new QueryBuilder()
          .select()
          .column("keyspace_name")
          .writeTimeColumn("durable_writes")
          .from("system_schema", "keyspaces")
          .build();

  private final StargateBridgeClient bridge;

  DefaultKeyspaceHelper(StargateBridgeClient bridge) {
    this.bridge = bridge;
  }

  CompletionStage<Optional<String>> find() {
    return getRows(new ArrayList<>(), null)
        .thenApply(
            rows -> {
              try {
                return rows.stream()
                    .filter(
                        r -> {
                          Value writeTime = r.getValues(1);
                          return !writeTime.hasNull() && Values.bigint(writeTime) > 0;
                        })
                    .min(Comparator.comparing(r -> Values.bigint(r.getValues(1))))
                    .map(r -> Values.string(r.getValues(0)))
                    .filter(
                        ks ->
                            !ks.equals("system")
                                && !ks.equals("data_endpoint_auth")
                                && !ks.equals("solr_admin")
                                && !ks.startsWith("system_")
                                && !ks.startsWith("dse_"));
              } catch (Exception e) {
                LOG.warn("Unexpected error while trying to find default keyspace", e);
                return Optional.empty();
              }
            });
  }

  private CompletionStage<List<Row>> getRows(List<Row> rows, BytesValue pagingState) {
    Query query =
        (pagingState == null)
            ? SELECT_KEYSPACE_NAMES
            : Query.newBuilder(SELECT_KEYSPACE_NAMES)
                .setParameters(QueryParameters.newBuilder().setPagingState(pagingState).build())
                .build();
    return bridge
        .executeQueryAsync(query)
        .thenCompose(
            response -> {
              ResultSet resultSet = response.getResultSet();
              rows.addAll(resultSet.getRowsList());
              return (resultSet.hasPagingState())
                  ? getRows(rows, resultSet.getPagingState())
                  : CompletableFuture.completedFuture(rows);
            });
  }
}
