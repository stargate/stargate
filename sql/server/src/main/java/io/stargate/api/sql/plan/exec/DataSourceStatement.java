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
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.javatuples.Pair;

/** Holds a prepared CQL statement. */
public class DataSourceStatement {

  public static final ConsistencyLevel DEFAULT_CL = ConsistencyLevel.LOCAL_QUORUM;

  private static final long PREPARE_TIMEOUT_MILLIS =
      Integer.getInteger("stargate.sql.prepare.timeout.millis", 120000);

  private final DataStore dataStore;
  private final Query<?> statement;
  private final List<Column> resultColumns;
  private final List<ColumnType> parameterTypes;

  private DataSourceStatement(
      DataStore dataStore,
      Query<?> prepared,
      List<Column> resultColumns,
      List<ColumnType> parameterTypes) {
    this.dataStore = dataStore;
    this.statement = prepared;
    this.resultColumns = resultColumns;
    this.parameterTypes = parameterTypes;
  }

  public List<Column> resultColumns() {
    return resultColumns;
  }

  public CompletableFuture<ResultSet> execute(Object... params) {
    List<Object> values = Collections.emptyList();
    if (params.length > 0) {
      if (parameterTypes.size() != params.length) {
        throw new IllegalArgumentException(
            String.format("Expected %d parameters, got %d", parameterTypes.size(), params.length));
      }

      values = new ArrayList<>(parameterTypes.size());
      int idx = 0;
      for (ColumnType type : parameterTypes) {
        Object value = params[idx++];
        values.add(TypeUtils.toDriverValue(value, type));
      }
    }

    BoundQuery bound = statement.bind(values);
    return dataStore.execute(bound, DEFAULT_CL);
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

    BuiltQuery<?> query = dataStore.queryBuilder().select().column(columns).from(table).build();

    CompletableFuture<? extends Query<?>> prepared = dataStore.prepare(query);

    return new DataSourceStatement(dataStore, get(prepared), columns, Collections.emptyList());
  }

  public static DataSourceStatement selectByKey(
      DataStore dataStore, StorageTable sqlTable, List<Column> keyColumns) {
    Table table = sqlTable.table();
    Pair<List<ColumnType>, List<BuiltCondition>> wheres = exactWhere(keyColumns);
    List<Column> columns = table.columns();

    BuiltQuery<?> query =
        dataStore
            .queryBuilder()
            .select()
            .column(columns)
            .from(table)
            .where(wheres.getValue1())
            .build();

    CompletableFuture<? extends Query<?>> prepared = dataStore.prepare(query);

    return new DataSourceStatement(dataStore, get(prepared), columns, wheres.getValue0());
  }

  public static DataSourceStatement upsert(
      DataStore dataStore, StorageTable sqlTable, List<Column> columns) {
    Table table = sqlTable.table();

    ImmutableList.Builder<ColumnType> types = ImmutableList.builder();
    ImmutableList.Builder<ValueModifier> values = ImmutableList.builder();
    for (Column c : columns) {
      types.add(Objects.requireNonNull(c.type()));
      values.add(ValueModifier.marker(c.name()));
    }

    List<ValueModifier> parameters = values.build();
    BuiltQuery<?> query = dataStore.queryBuilder().insertInto(table).value(parameters).build();

    CompletableFuture<? extends Query<?>> prepared = dataStore.prepare(query);

    return new DataSourceStatement(
        dataStore, get(prepared), Collections.emptyList(), types.build());
  }

  public static DataSourceStatement delete(
      DataStore dataStore, StorageTable sqlTable, List<Column> keyColumns) {
    Table table = sqlTable.table();
    Pair<List<ColumnType>, List<BuiltCondition>> wheres = exactWhere(keyColumns);

    BuiltQuery<?> query =
        dataStore.queryBuilder().delete().from(table).where(wheres.getValue1()).build();

    CompletableFuture<? extends Query<?>> prepared = dataStore.prepare(query);

    return new DataSourceStatement(
        dataStore, get(prepared), Collections.emptyList(), wheres.getValue0());
  }

  private static Pair<List<ColumnType>, List<BuiltCondition>> exactWhere(List<Column> columns) {
    ImmutableList.Builder<ColumnType> types = ImmutableList.builder();
    ImmutableList.Builder<BuiltCondition> conditions = ImmutableList.builder();
    for (Column c : columns) {
      types.add(Objects.requireNonNull(c.type()));
      conditions.add(BuiltCondition.ofMarker(c.name(), Predicate.EQ));
    }

    return Pair.with(types.build(), conditions.build());
  }
}
