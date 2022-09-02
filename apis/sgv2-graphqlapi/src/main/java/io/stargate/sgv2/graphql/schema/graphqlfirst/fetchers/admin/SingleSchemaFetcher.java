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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin;

import com.google.common.annotations.VisibleForTesting;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public class SingleSchemaFetcher extends SchemaFetcher<SchemaSource> {
  private final Function<StargateBridgeClient, SchemaSourceDao> schemaSourceDaoProvider;

  public SingleSchemaFetcher() {
    this(SchemaSourceDao::new);
  }

  @VisibleForTesting
  public SingleSchemaFetcher(
      Function<StargateBridgeClient, SchemaSourceDao> schemaSourceDaoProvider) {
    this.schemaSourceDaoProvider = schemaSourceDaoProvider;
  }

  @Override
  public SchemaSource get(DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws Exception {
    StargateBridgeClient bridge = context.getBridge();
    String keyspace = getKeyspace(environment, bridge);
    Optional<UUID> version =
        Optional.ofNullable((String) environment.getArgument("version")).map(UUID::fromString);
    return schemaSourceDaoProvider.apply(bridge).getSingleVersion(keyspace, version).orElse(null);
  }
}
