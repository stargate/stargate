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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.api.common.grpc;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.commons.codec.binary.Hex;

@ApplicationScoped
public class StargateBridgeClientImpl implements StargateBridgeClient {

  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE = ks -> Uni.createFrom().failure(MissingKeyspaceException.INSTANCE);

  @Inject SchemaManager schemaManager;

  @Inject StargateRequestInfo requestInfo;

  @Override
  public CompletionStage<QueryOuterClass.Response> executeQueryAsync(QueryOuterClass.Query query) {
    return requestInfo.getStargateBridge().executeQuery(query).subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<QueryOuterClass.Response> executeQueryAsync(
      String keyspaceName,
      String tableName,
      Function<Optional<Schema.CqlTable>, QueryOuterClass.Query> queryProducer) {

    // TODO implement optimistic queries (probably requires changes directly in SchemaManager)
    return getTableAsync(keyspaceName, tableName, true)
        .thenCompose(table -> executeQueryAsync(queryProducer.apply(table)));
  }

  @Override
  public CompletionStage<QueryOuterClass.Response> executeBatchAsync(QueryOuterClass.Batch batch) {
    return requestInfo.getStargateBridge().executeBatch(batch).subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Optional<Schema.CqlKeyspaceDescribe>> getKeyspaceAsync(
      String keyspaceName, boolean checkIfAuthorized) {
    Uni<Schema.CqlKeyspaceDescribe> keyspace =
        checkIfAuthorized
            ? schemaManager.getKeyspaceAuthorized(keyspaceName)
            : schemaManager.getKeyspace(keyspaceName);
    return keyspace.map(Optional::ofNullable).subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<List<Schema.CqlKeyspaceDescribe>> getAllKeyspacesAsync() {
    return schemaManager.getKeyspaces().collect().asList().subscribeAsCompletionStage();
  }

  @Override
  public String decorateKeyspaceName(String keyspaceName) {
    return requestInfo
        .getTenantId()
        .map(
            tenantId ->
                Hex.encodeHexString(tenantId.getBytes(StandardCharsets.UTF_8)) + "_" + keyspaceName)
        .orElse(keyspaceName);
  }

  @Override
  public CompletionStage<Optional<Schema.CqlTable>> getTableAsync(
      String keyspaceName, String tableName, boolean checkIfAuthorized) {
    Uni<Schema.CqlTable> table =
        checkIfAuthorized
            ? schemaManager.getTableAuthorized(keyspaceName, tableName, MISSING_KEYSPACE)
            : schemaManager.getTable(keyspaceName, tableName, MISSING_KEYSPACE);
    return table
        .onFailure(MissingKeyspaceException.class)
        .recoverWithNull()
        .map(Optional::ofNullable)
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<List<Schema.CqlTable>> getTablesAsync(String keyspaceName) {
    return schemaManager
        .getTables(keyspaceName, MISSING_KEYSPACE)
        .collect()
        .asList()
        .onFailure(MissingKeyspaceException.class)
        .recoverWithItem(Collections.emptyList())
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<List<Boolean>> authorizeSchemaReadsAsync(
      List<Schema.SchemaRead> schemaReads) {
    return requestInfo
        .getStargateBridge()
        .authorizeSchemaReads(
            Schema.AuthorizeSchemaReadsRequest.newBuilder().addAllSchemaReads(schemaReads).build())
        .map(Schema.AuthorizeSchemaReadsResponse::getAuthorizedList)
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Schema.SupportedFeaturesResponse> getSupportedFeaturesAsync() {
    return requestInfo
        .getStargateBridge()
        .getSupportedFeatures(Schema.SupportedFeaturesRequest.getDefaultInstance())
        .subscribeAsCompletionStage();
  }

  // Hack: we need to recover from a missing keyspace as empty optional/list, but SchemaManager only
  // handles exceptions properly.
  private static class MissingKeyspaceException extends RuntimeException {
    private static final MissingKeyspaceException INSTANCE = new MissingKeyspaceException();
  }
}
