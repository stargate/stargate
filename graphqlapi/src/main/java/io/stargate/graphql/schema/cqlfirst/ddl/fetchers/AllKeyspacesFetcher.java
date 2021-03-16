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
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllKeyspacesFetcher extends CassandraFetcher<List<KeyspaceDto>> {

  private static final Logger LOG = LoggerFactory.getLogger(AllKeyspacesFetcher.class);

  public AllKeyspacesFetcher(
      AuthorizationService authorizationService, DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory);
  }

  @Override
  protected List<KeyspaceDto> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject) {

    return dataStore.schema().keyspaces().stream()
        .filter(
            keyspace -> {
              try {
                authorizationService.authorizeSchemaRead(
                    authenticationSubject,
                    Collections.singletonList(keyspace.name()),
                    null,
                    SourceAPI.GRAPHQL);
                return true;
              } catch (UnauthorizedException e) {
                LOG.debug("Not returning keyspace {} due to not being authorized", keyspace.name());
                return false;
              }
            })
        .map(keyspace -> new KeyspaceDto(keyspace, authorizationService, authenticationSubject))
        .collect(Collectors.toList());
  }
}
