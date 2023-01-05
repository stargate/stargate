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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema.SchemaRead;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.api.common.grpc.proto.SchemaReads;
import java.util.concurrent.CompletionStage;

public class StargateGraphqlResourceBase extends GraphqlResourceBase {

  protected final StargateBridgeClient bridge;
  protected final GraphqlCache graphqlCache;

  public StargateGraphqlResourceBase(
      ObjectMapper objectMapper, StargateBridgeClient bridge, GraphqlCache graphqlCache) {
    super(objectMapper);
    this.bridge = bridge;
    this.graphqlCache = graphqlCache;
  }

  protected StargateGraphqlContext newContext() {
    return new StargateGraphqlContext(bridge, graphqlCache);
  }

  protected Uni<Boolean> isAuthorized(String keyspaceName) {
    SchemaRead schemaRead = SchemaReads.keyspace(keyspaceName);
    CompletionStage<Boolean> result = bridge.authorizeSchemaReadAsync(schemaRead);
    return Uni.createFrom().future(result.toCompletableFuture());
  }
}
