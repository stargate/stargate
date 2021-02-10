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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.*;
import org.junit.jupiter.api.Test;

class SingleSchemaFetcherTest {
  @Test
  public void shouldGetOneSchemaEntry() throws Exception {
    // given
    String namespace = "ns_1";
    SchemaSource schemaSource = new SchemaSource(namespace, Uuids.timeBased(), "content");

    SchemaSourceDao schemaSourceDao = mock(SchemaSourceDao.class);
    when(schemaSourceDao.getLatest(namespace)).thenReturn(schemaSource);
    SingleSchemaFetcher singleSchemaFetcher =
        new SingleSchemaFetcher(
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(DataStoreFactory.class),
            (ds) -> schemaSourceDao);

    DataFetchingEnvironment dataFetchingEnvironment = mockDataFetchingEnvironment(namespace);
    DataStore dataStore = mockDataStore(namespace);

    // when
    Map<String, Object> result =
        singleSchemaFetcher.get(
            dataFetchingEnvironment, dataStore, mock(AuthenticationSubject.class));

    // then
    assertThat(result)
        .contains(
            new AbstractMap.SimpleEntry<>("namespace", namespace),
            new AbstractMap.SimpleEntry<>("version", schemaSource.getVersion()),
            new AbstractMap.SimpleEntry<>("deployDate", schemaSource.getDeployDate()));
  }

  private DataFetchingEnvironment mockDataFetchingEnvironment(String namespace) {
    DataFetchingEnvironment dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    when(dataFetchingEnvironment.getArgument("namespace")).thenReturn(namespace);
    return dataFetchingEnvironment;
  }

  private DataStore mockDataStore(String namespace) {
    DataStore dataStore = mock(DataStore.class);
    Schema schema = mock(Schema.class);
    when(dataStore.schema()).thenReturn(schema);
    when(schema.keyspaceNames()).thenReturn(Collections.singletonList(namespace));
    return dataStore;
  }
}
