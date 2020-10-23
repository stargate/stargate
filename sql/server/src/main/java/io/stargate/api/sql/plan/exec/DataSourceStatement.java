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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.api.sql.schema.StorageTable;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.query.ImmutableWhereCondition;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.datastore.query.WhereCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Holds a prepared CQL statement. */
public class DataSourceStatement {

  public static final ConsistencyLevel DEFAULT_CL = ConsistencyLevel.LOCAL_QUORUM;

  private static final long PREPARE_TIMEOUT_MILLIS =
      Integer.getInteger("stargate.sql.prepare.timeout.millis", 120000);

  private final DataStore dataStore;
  private final PreparedStatement statement;
  private final List<Column> resultColumns;

  private DataSourceStatement(
      DataStore dataStore, PreparedStatement prepared, List<Column> resultColumns) {
    this.dataStore = dataStore;
    this.statement = prepared;
    this.resultColumns = resultColumns;
  }

  public List<Column> resultColumns() {
    return resultColumns;
  }

  public CompletableFuture<ResultSet> execute(Object... params) {
    return statement.execute(DEFAULT_CL, params);
  }

  private static Object bind(Column.ColumnType type, int idx, Object[] params) {
    Object value = params[idx];
    return TypeUtils.toDriverValue(value, type);
  }

  private static <T> T get(CompletableFuture<T> future) {
    try {
      return future.get(PREPARE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static DataSourceStatement fullScan(DataStore dataStore, StorageTable sqlTable) {
    Table table = sqlTable.table();
    List<Column> columns = table.columns();

    CompletableFuture<PreparedStatement> prepared =
        dataStore
            .query()
            .select()
            .column(columns)
            .from(sqlTable.keyspace(), table)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, get(prepared), columns);
  }

  public static DataSourceStatement selectByKey(
      DataStore dataStore, StorageTable sqlTable, List<Column> keyColumns) {
    Table table = sqlTable.table();
    List<Where<?>> wheres = exactWhere(keyColumns);
    List<Column> columns = table.columns();

    CompletableFuture<PreparedStatement> prepared =
        dataStore
            .query()
            .select()
            .column(columns)
            .from(sqlTable.keyspace(), table)
            .where(wheres)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, get(prepared), columns);
  }

  public static DataSourceStatement upsert(
      DataStore dataStore, StorageTable sqlTable, List<Column> columns) {
    Table table = sqlTable.table();

    ImmutableList.Builder<Value<?>> list = ImmutableList.builder();
    int idx = 0;
    for (Column c : columns) {
      int i = idx++;
      Column.ColumnType type = Objects.requireNonNull(c.type());
      list.add(Value.<Object[]>create(c, p -> bind(type, i, p)));
    }
    List<Value<?>> values = list.build();

    CompletableFuture<PreparedStatement> prepared =
        dataStore
            .query()
            .insertInto(sqlTable.keyspace(), table)
            .value(values)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, get(prepared), Collections.emptyList());
  }

  public static DataSourceStatement delete(
      DataStore dataStore, StorageTable sqlTable, List<Column> keyColumns) {
    Table table = sqlTable.table();
    List<Where<?>> wheres = exactWhere(keyColumns);

    CompletableFuture<PreparedStatement> prepared =
        dataStore
            .query()
            .delete()
            .from(sqlTable.keyspace(), table)
            .where(wheres)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, get(prepared), Collections.emptyList());
  }

  private static List<Where<?>> exactWhere(List<Column> columns) {
    ImmutableList.Builder<Where<?>> list = ImmutableList.builder();
    int idx = 0;
    for (Column c : columns) {
      int i = idx++;
      Column.ColumnType type = Objects.requireNonNull(c.type());
      list.add(
          ImmutableWhereCondition.<Object[]>builder()
              .column(c)
              .bindingFunction(p -> bind(type, i, p))
              .predicate(WhereCondition.Predicate.Eq)
              .build());
    }

    return list.build();
  }
}
