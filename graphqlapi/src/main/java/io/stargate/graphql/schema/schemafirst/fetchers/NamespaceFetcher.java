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
package io.stargate.graphql.schema.schemafirst.fetchers;

import com.google.common.collect.ImmutableMap;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NamespaceFetcher<ResultT> extends CassandraFetcher<ResultT> {

  private static final Logger LOG = LoggerFactory.getLogger(AllNamespacesFetcher.class);

  protected NamespaceFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  protected boolean isReadAuthorized(
      Keyspace keyspace, AuthenticationSubject authenticationSubject) {
    try {
      authorizationService.authorizeSchemaRead(
          authenticationSubject,
          Collections.singletonList(keyspace.name()),
          null,
          SourceAPI.GRAPHQL);
    } catch (UnauthorizedException e) {
      LOG.debug("Not returning keyspace {} due to not being authorized", keyspace.name());
      return false;
    }
    return true;
  }

  protected ImmutableMap<String, Object> formatNamespace(Keyspace keyspace) {
    List<Map<String, String>> dcs = new ArrayList<>();
    Map<String, String> replication = keyspace.replication();
    if ("org.apache.cassandra.locator.SimpleStrategy".equals(replication.get("class"))) {
      String replicasSpec = replication.get("replication_factor");
      try {
        int replicas = Integer.parseInt(replicasSpec);
        return ImmutableMap.of("name", keyspace.name(), "replicas", replicas);
      } catch (NumberFormatException e) {
        LOG.warn(
            "Couldn't parse replication factor for keyspace {}: {}", keyspace.name(), replicasSpec);
        return ImmutableMap.of("name", keyspace.name());
      }
    } else {
      for (Map.Entry<String, String> entry : replication.entrySet()) {
        if (!entry.getKey().equals("class") && !entry.getKey().equals("replication_factor")) {
          dcs.add(
              ImmutableMap.of(
                  "name", entry.getKey(),
                  "replicas", entry.getValue()));
        }
      }
      return ImmutableMap.of("name", keyspace.name(), "datacenters", dcs);
    }
  }
}
