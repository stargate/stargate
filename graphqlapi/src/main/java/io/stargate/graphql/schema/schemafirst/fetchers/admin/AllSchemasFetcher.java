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
package io.stargate.graphql.schema.schemafirst.fetchers.admin;

import com.google.common.annotations.VisibleForTesting;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import java.util.*;
import java.util.function.Function;

public class AllSchemasFetcher extends SchemaFetcher<List<Map<String, Object>>> {
  private final Function<DataStore, SchemaSourceDao> schemaSourceDaoProvider;

  public AllSchemasFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this(authenticationService, authorizationService, dataStoreFactory, (SchemaSourceDao::new));
  }

  @VisibleForTesting
  public AllSchemasFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      Function<DataStore, SchemaSourceDao> schemaSourceDaoProvider) {
    super(authenticationService, authorizationService, dataStoreFactory);
    this.schemaSourceDaoProvider = schemaSourceDaoProvider;
  }

  @Override
  protected List<Map<String, Object>> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {
    String namespace = getNamespace(environment, dataStore);

    authorize(authenticationSubject, namespace);

    List<SchemaSource> schemas =
        schemaSourceDaoProvider.apply(dataStore).getSchemaHistory(namespace);
    List<Map<String, Object>> result = new ArrayList<>(schemas.size());

    for (SchemaSource source : schemas) {
      Map<String, Object> singleResult = schemaSourceToMap(namespace, source);
      result.add(singleResult);
    }
    return result;
  }
}
