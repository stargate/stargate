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
package io.stargate.graphql.schema.fetchers.ddl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateKeyspaceFetcher extends DdlQueryFetcher {

  public CreateKeyspaceFetcher(
      Persistence persistence, AuthenticationService authenticationService) {
    super(persistence, authenticationService);
  }

  @Override
  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    CreateKeyspaceStart start =
        SchemaBuilder.createKeyspace(
            CqlIdentifier.fromInternal(dataFetchingEnvironment.getArgument("name")));
    boolean ifNotExists =
        dataFetchingEnvironment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    if (ifNotExists) {
      start = start.ifNotExists();
    }
    Integer replicas = dataFetchingEnvironment.getArgument("replicas");
    List<Map<String, Object>> datacenters = dataFetchingEnvironment.getArgument("datacenters");
    if (replicas == null && datacenters == null) {
      throw new IllegalArgumentException("You must specify either replicas or datacenters");
    }
    if (replicas != null && datacenters != null) {
      throw new IllegalArgumentException("You can't specify both replicas and datacenters");
    }

    if (replicas != null) {
      return start.withSimpleStrategy(replicas).asCql();
    } else { // datacenters != null
      return start.withNetworkTopologyStrategy(parseDatacenters(datacenters)).asCql();
    }
  }

  private Map<String, Integer> parseDatacenters(List<Map<String, Object>> datacenters) {
    assert datacenters != null;
    if (datacenters.isEmpty()) {
      throw new IllegalArgumentException("datacenters must contain at least one element");
    }
    Map<String, Integer> result = new HashMap<>();
    for (Map<String, Object> datacenter : datacenters) {
      String dcName = (String) datacenter.get("name");
      Integer dcReplicas = (Integer) datacenter.getOrDefault("replicas", 3);
      result.put(dcName, dcReplicas);
    }
    return result;
  }
}
