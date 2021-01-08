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
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SchemaFetcher extends CassandraFetcher<Map<String, Object>> {

  public SchemaFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {

    String namespace = environment.getArgument("namespace");
    if (!dataStore.schema().keyspaceNames().contains(namespace)) {
      throw new IllegalArgumentException(
          String.format("Namespace '%s' does not exist.", namespace));
    }

    authorizationService.authorizeSchemaRead(
        authenticationSubject,
        Collections.singletonList(namespace),
        Collections.singletonList(SchemaSourceDao.TABLE_NAME),
        SourceAPI.GRAPHQL);

    ImmutableMap.Builder<String, Object> result = ImmutableMap.builder();
    result.put("namespace", namespace);
    SchemaSource source = new SchemaSourceDao(dataStore).getLatest(namespace);
    if (source != null) {
      UUID version = source.getVersion();
      ZonedDateTime deployDate = source.getDeployDate();
      String contents =
          String.format("# Schema for '%s', version %s (%s)\n\n", namespace, version, deployDate)
              + source.getContents();
      result.put("version", version);
      result.put("deployDate", deployDate);
      result.put("contents", contents);
    }
    ;
    return result.build();
  }
}
