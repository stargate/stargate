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

import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.UUID;

public class UndeploySchemaFetcher extends CassandraFetcher<Boolean> {

  @Override
  protected Boolean get(DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws Exception {

    String keyspaceName = environment.getArgument("keyspace");
    Schema.CqlKeyspaceDescribe keyspace =
        context
            .getBridge()
            .getKeyspace(keyspaceName, true)
            .orElseThrow(() -> new IllegalArgumentException("Keyspace '%s' does not exist."));

    UUID expectedVersion = getExpectedVersion(environment);
    boolean force = environment.getArgument("force");

    new SchemaSourceDao(context.getBridge()).undeploy(keyspaceName, expectedVersion, force);
    return true;
  }

  private UUID getExpectedVersion(DataFetchingEnvironment environment) {
    // Unlike deploy, the field is mandatory.
    try {
      return UUID.fromString(environment.getArgument("expectedVersion"));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid 'expectedVersion' value.");
    }
  }
}
