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
package io.stargate.db.cdc.datastore;

import static io.stargate.db.cdc.config.CDCConfigLoader.CDC_CORE_CONFIG_MODULE_NAME;
import static io.stargate.db.cdc.config.CDCConfigLoader.ENABLED_TABLES_SETTINGS_NAME;
import static org.mockito.Mockito.*;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.db.BatchType;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.*;
import io.stargate.db.schema.Table;
import java.util.*;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class CDCEnabledDataStoreTest {

  @Test
  public void shouldCreateLoggedBatchIfBoundDMLQueryTrackedByCDCUsingExecute() {
    // given
    DataStore dataStore = mock(DataStore.class);
    ConfigStore configStore = configStoreWithCDCTracking("keyspace", "table");
    CDCQueryBuilder cdcQueryBuilder = mock(CDCQueryBuilder.class);
    CDCEnabledDataStore cdcEnabledDataStore =
        new CDCEnabledDataStore(dataStore, configStore, cdcQueryBuilder);
    BoundDMLQuery boundDMLQuery = mockBoundDMLQuery("keyspace", "table");
    BoundInsert cdcQueryInsert = mock(BoundInsert.class);
    when(cdcQueryBuilder.toInsert(boundDMLQuery)).thenReturn(cdcQueryInsert);

    // when
    cdcEnabledDataStore.execute(boundDMLQuery);

    // then
    verify(dataStore, times(1))
        .batch(eq(Arrays.asList(boundDMLQuery, cdcQueryInsert)), eq(BatchType.LOGGED), any());
    verify(dataStore, times(0)).execute(eq(boundDMLQuery), any(UnaryOperator.class));
  }

  @Test
  public void shouldNotCreateLoggedBatchIfBoundDMLNotQueryTrackedByCDCUsingExecute() {
    // given
    DataStore dataStore = mock(DataStore.class);
    ConfigStore configStore = configStoreWithCDCTracking("keyspace_not_tracked", "table");
    CDCQueryBuilder cdcQueryBuilder = mock(CDCQueryBuilder.class);
    CDCEnabledDataStore cdcEnabledDataStore =
        new CDCEnabledDataStore(dataStore, configStore, cdcQueryBuilder);
    BoundDMLQuery boundDMLQuery = mockBoundDMLQuery("keyspace", "table");
    BoundInsert cdcQueryInsert = mock(BoundInsert.class);
    when(cdcQueryBuilder.toInsert(boundDMLQuery)).thenReturn(cdcQueryInsert);

    // when
    cdcEnabledDataStore.execute(boundDMLQuery);

    // then
    verify(dataStore, times(0))
        .batch(eq(Arrays.asList(boundDMLQuery, cdcQueryInsert)), eq(BatchType.LOGGED), any());
    verify(dataStore, times(1)).execute(eq(boundDMLQuery), any(UnaryOperator.class));
  }

  @Test
  public void shouldCreateQueryForEachBoundDMLQueryTrackedByCDCUsingBatch() {
    // given
    DataStore dataStore = mock(DataStore.class);
    ConfigStore configStore = configStoreWithCDCTracking("keyspace", "table");
    CDCQueryBuilder cdcQueryBuilder = mock(CDCQueryBuilder.class);
    CDCEnabledDataStore cdcEnabledDataStore =
        new CDCEnabledDataStore(dataStore, configStore, cdcQueryBuilder);
    BoundDMLQuery boundDMLQuery = mockBoundDMLQuery("keyspace", "table");
    BoundDMLQuery boundDMLQuery2 = mockBoundDMLQuery("keyspace", "table");
    BoundDMLQuery boundDMLQueryNotTracked = mockBoundDMLQuery("keyspace", "not_tracked_table");
    BoundInsert cdcQueryInsert = mock(BoundInsert.class);
    BoundInsert cdcQueryInsert2 = mock(BoundInsert.class);
    when(cdcQueryBuilder.toInsert(boundDMLQuery)).thenReturn(cdcQueryInsert);
    when(cdcQueryBuilder.toInsert(boundDMLQuery2)).thenReturn(cdcQueryInsert2);

    // when
    cdcEnabledDataStore.batch(
        Arrays.asList(boundDMLQuery, boundDMLQuery2, boundDMLQueryNotTracked));

    // then
    verify(dataStore, times(1))
        .batch(
            eq(
                Arrays.asList(
                    boundDMLQuery,
                    boundDMLQuery2,
                    boundDMLQueryNotTracked,
                    cdcQueryInsert,
                    cdcQueryInsert2)),
            eq(BatchType.LOGGED),
            any());
  }

  @Test
  public void shouldNotCreateAnyQueryIfBoundDMLQueryNotTrackedByCDCUsingBatch() {
    // given
    DataStore dataStore = mock(DataStore.class);
    ConfigStore configStore = configStoreWithCDCTracking("keyspace", "table");
    CDCQueryBuilder cdcQueryBuilder = mock(CDCQueryBuilder.class);
    CDCEnabledDataStore cdcEnabledDataStore =
        new CDCEnabledDataStore(dataStore, configStore, cdcQueryBuilder);
    BoundDMLQuery boundDMLQuery = mockBoundDMLQuery("keyspace", "table_not_tracked");

    // when
    cdcEnabledDataStore.batch(Collections.singletonList(boundDMLQuery));

    // then
    verify(dataStore, times(1)).batch(eq(Arrays.asList(boundDMLQuery)), any(), any());
  }

  private BoundDMLQuery mockBoundDMLQuery(String keyspace, String table) {
    BoundDMLQuery boundDMLQuery = mock(BoundDMLQuery.class);
    Table queryTable = mock(Table.class);
    when(queryTable.name()).thenReturn(table);
    when(queryTable.keyspace()).thenReturn(keyspace);
    when(boundDMLQuery.table()).thenReturn(queryTable);
    return boundDMLQuery;
  }

  private ConfigStore configStoreWithCDCTracking(String keyspace, String table) {
    ConfigStore configStore = mock(ConfigStore.class);
    Map<String, Object> settings = new HashMap<>();
    settings.put(
        ENABLED_TABLES_SETTINGS_NAME,
        Collections.singletonList(String.format("%s.%s", keyspace, table)));
    when(configStore.getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME))
        .thenReturn(new ConfigWithOverrides(settings, CDC_CORE_CONFIG_MODULE_NAME));
    return configStore;
  }
}
