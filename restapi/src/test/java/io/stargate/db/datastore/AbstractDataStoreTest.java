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
package io.stargate.db.datastore;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import io.stargate.db.datastore.ValidatingDataStore.QueryExpectation;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractDataStoreTest {

  private ValidatingDataStore dataStore;

  protected abstract Schema schema();

  protected DataStore datastore() {
    return dataStore;
  }

  @BeforeEach
  public void createDataStore() {
    dataStore = new ValidatingDataStore(schema());
    dataStore.reset();
  }

  @AfterEach
  public void checkExpectedExecutions() {
    dataStore.validate();
  }

  protected <T> List<T> values(Flowable<T> flowable) {
    TestSubscriber<T> test = flowable.test();
    test.assertNoErrors();
    return test.values();
  }

  protected void ignorePreparedExecutions() {
    dataStore.ignorePrepared();
  }

  protected QueryExpectation withQuery(Table table, String cql, Object... params) {
    return dataStore.withQuery(table, cql, params);
  }

  protected QueryExpectation withAnySelectFrom(Table table) {
    return dataStore.withAnySelectFrom(table);
  }

  protected QueryExpectation withAnyUpdateOf(Table table) {
    return dataStore.withAnyUpdateOf(table);
  }

  protected QueryExpectation withAnyInsertInfo(Table table) {
    return dataStore.withAnyInsertInfo(table);
  }
}
