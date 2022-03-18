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
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the oldest, non-system keyspace in the database. It is used as a default when no keyspace
 * name is provided in the URL path.
 *
 * @see DmlResource#get(java.lang.String, java.lang.String, java.lang.String,
 *     javax.servlet.http.HttpServletRequest, javax.ws.rs.container.AsyncResponse,
 *     io.stargate.sgv2.common.grpc.StargateBridgeClient)
 * @see DmlResource#postJson(io.stargate.sgv2.graphql.web.models.GraphqlJsonBody, java.lang.String,
 *     javax.servlet.http.HttpServletRequest, javax.ws.rs.container.AsyncResponse,
 *     io.stargate.sgv2.common.grpc.StargateBridgeClient)
 * @see DmlResource#postGraphql(java.lang.String, javax.servlet.http.HttpServletRequest,
 *     java.lang.String, javax.ws.rs.container.AsyncResponse,
 *     io.stargate.sgv2.common.grpc.StargateBridgeClient)
 */
class DefaultKeyspaceHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultKeyspaceHelper.class);
  private static final Query QUERY =
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

  Optional<String> find() {
    try {
      return getRows().stream()
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
  }

  private List<Row> getRows() {
    ResultSet resultSet = bridge.executeQuery(QUERY).getResultSet();
    // Optimize for simple (and likely) case where there is only one page
    if (!resultSet.hasPagingState()) {
      return resultSet.getRowsList();
    }
    List<Row> rows = new ArrayList<>(resultSet.getRowsList());
    do {
      BytesValue pagingState = resultSet.getPagingState();
      QueryParameters.Builder newParameters =
          QueryParameters.newBuilder(QUERY.getParameters()).setPagingState(pagingState);
      resultSet =
          bridge
              .executeQuery(Query.newBuilder(QUERY).setParameters(newParameters).build())
              .getResultSet();
      rows.addAll(resultSet.getRowsList());
    } while (resultSet.hasPagingState());
    return rows;
  }
}
