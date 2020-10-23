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
package io.stargate.api.sql.plan.exec;

import com.google.common.collect.ImmutableMap;
import io.stargate.api.sql.schema.StorageTable;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

/** Contains SQL query parameters and related {@link DataSourceStatement} objects (CQL queries). */
public class RuntimeContext implements DataContext {

  private final DataStore dataStore;
  private final Map<String, DataSourceStatement> statements;
  private final Map<String, Object> parameters;

  private RuntimeContext(
      DataStore dataStore,
      Map<String, DataSourceStatement> statements,
      Map<String, Object> parameters) {
    this.dataStore = dataStore;
    this.statements = statements;
    this.parameters = parameters;
  }

  public RuntimeContext withPositionalParameters(List<?> parameterList) {
    ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
    map.putAll(parameters);

    int idx = 0;
    for (Object value : parameterList) {
      // Note: we match Calcite's way of constructing parameter names here.
      // Cf. org.apache.calcite.rex.RexDynamicParam
      // Cf. org.apache.calcite.jdbc.CalciteConnectionImpl.enumerable(...)
      map.put("?" + (idx++), value);
    }

    return new RuntimeContext(dataStore, statements, map.build());
  }

  @Override
  public SchemaPlus getRootSchema() {
    throw new UnsupportedOperationException();
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryProvider getQueryProvider() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(String name) {
    return parameters.get(name);
  }

  public DataStore dataStore() {
    return dataStore;
  }

  public DataSourceStatement statement(String statementId) {
    return statements.get(statementId);
  }

  public static class Builder {
    private final DataStore dataStore;
    private final AtomicInteger seq = new AtomicInteger();
    private final ImmutableMap.Builder<String, DataSourceStatement> statements =
        ImmutableMap.builder();

    public Builder(DataStore dataStore) {
      this.dataStore = dataStore;
    }

    private String add(DataSourceStatement statement) {
      String id = "__ds__s" + seq.incrementAndGet();
      statements.put(id, statement);
      return id;
    }

    public String newFullScan(StorageTable table) {
      return add(DataSourceStatement.fullScan(dataStore, table));
    }

    public String newSelectByKey(StorageTable table, List<Column> keyColumns) {
      return add(DataSourceStatement.selectByKey(dataStore, table, keyColumns));
    }

    public String newUpsert(StorageTable table, List<Column> columns) {
      return add(DataSourceStatement.upsert(dataStore, table, columns));
    }

    public String newDelete(StorageTable table, List<Column> keyColumns) {
      return add(DataSourceStatement.delete(dataStore, table, keyColumns));
    }

    public RuntimeContext build() {
      return new RuntimeContext(dataStore, statements.build(), Collections.emptyMap());
    }
  }
}
