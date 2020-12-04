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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.cdc.config.CDCConfig;
import io.stargate.db.cdc.config.CDCConfigLoader;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

public class CDCEnabledDataStore implements DataStore {

  private final DataStore dataStore;
  private final CDCConfig cdcConfig;
  private final CDCQueryBuilder cdcQueryBuilder;

  public CDCEnabledDataStore(DataStore dataStore, ConfigStore configStore) {
    this(dataStore, configStore, new CDCQueryBuilder());
  }

  @VisibleForTesting
  public CDCEnabledDataStore(
      DataStore dataStore, ConfigStore configStore, CDCQueryBuilder cdcQueryBuilder) {
    this.dataStore = dataStore;
    this.cdcConfig = CDCConfigLoader.loadConfig(configStore);
    this.cdcQueryBuilder = cdcQueryBuilder;
  }

  @Override
  public TypedValue.Codec valueCodec() {
    return dataStore.valueCodec();
  }

  @Override
  public <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query) {
    return dataStore.prepare(query);
  }

  @Override
  public Schema schema() {
    return dataStore.schema();
  }

  @Override
  public boolean isInSchemaAgreement() {
    return dataStore.isInSchemaAgreement();
  }

  @Override
  public void waitForSchemaAgreement() {
    dataStore.waitForSchemaAgreement();
  }

  @Override
  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    if (isMutationTrackedByCDC(query)) {
      return dataStore.batch(
          Arrays.asList(query, cdcQueryBuilder.toInsert(query)),
          BatchType.LOGGED,
          parametersModifier);
    } else {
      return dataStore.execute(query, parametersModifier);
    }
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier) {
    Collection<BoundQuery> cdcInsertQueries = new ArrayList<>();
    for (BoundQuery query : queries) {
      if (isMutationTrackedByCDC(query)) {
        cdcInsertQueries.add(cdcQueryBuilder.toInsert(query));
      }
    }
    if (cdcInsertQueries.isEmpty()) {
      return dataStore.batch(queries, batchType, parametersModifier);
    } else {
      Collection<BoundQuery> originalAndCDCQueries = new ArrayList<>();
      originalAndCDCQueries.addAll(queries);
      originalAndCDCQueries.addAll(cdcInsertQueries);
      return dataStore.batch(originalAndCDCQueries, BatchType.LOGGED, parametersModifier);
    }
  }

  private boolean isMutationTrackedByCDC(BoundQuery query) {
    return query instanceof BoundDMLQuery
        && cdcConfig.isTrackedByCDC(((BoundDMLQuery) query).table());
  }
}
