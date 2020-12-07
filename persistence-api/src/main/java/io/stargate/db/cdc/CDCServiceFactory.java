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
package io.stargate.db.cdc;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.cdc.datastore.CDCQueryBuilder;
import io.stargate.db.datastore.DataStore;

public class CDCServiceFactory {
  private final CDCQueryBuilder cdcQueryBuilder;

  public CDCServiceFactory(ConfigStore configStore, DataStore dataStore) {
    this.cdcQueryBuilder = new CDCQueryBuilder(configStore, dataStore);
  }

  public void create() {
    cdcQueryBuilder.initCDCKeyspace();
    cdcQueryBuilder.initCDCEventsTable();
  }
}
