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
package io.stargate.sgv2.graphql.integration.util;

import io.stargate.sgv2.common.IntegrationTestUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;

public abstract class GraphqlFirstIntegrationTest extends GraphqlIntegrationTest {

  protected GraphqlFirstClient client;

  @BeforeAll
  public void createClient() {
    client = new GraphqlFirstClient(baseUrl, IntegrationTestUtils.getAuthToken());
  }

  protected void deleteAllGraphqlSchemas() {
    session
        .getMetadata()
        .getKeyspace("stargate_graphql")
        .flatMap(ks -> ks.getTable("schema_source"))
        .ifPresent(
            __ ->
                session.execute(
                    "DELETE FROM stargate_graphql.schema_source WHERE keyspace_name = ?",
                    keyspaceId.asInternal()));
  }

  @SuppressWarnings("unchecked")
  protected String getMappingErrors(Map<String, Object> errors) {
    Map<String, Object> value =
        ((Map<String, List<Map<String, Object>>>) errors.get("extensions"))
            .get("mappingErrors")
            .get(0);
    return (String) value.get("message");
  }
}
