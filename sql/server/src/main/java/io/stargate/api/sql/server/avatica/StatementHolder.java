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
package io.stargate.api.sql.server.avatica;

import com.google.common.collect.ImmutableList;
import io.stargate.api.sql.plan.PreparedSqlQuery;
import io.stargate.api.sql.plan.QueryPlanner;
import io.stargate.db.datastore.DataStore;
import java.lang.reflect.Type;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class StatementHolder extends Meta.StatementHandle {
  private static final QueryPlanner queryPlanner = new QueryPlanner();

  private final Prepared prepared;
  private final DataStore dataStore;
  private volatile ExecutionResult result;

  private StatementHolder(String connectionId, int id, Prepared prepared, DataStore dataStore) {
    super(connectionId, id, prepared.signature());
    this.prepared = prepared;
    this.dataStore = dataStore;
  }

  public static StatementHolder prepare(
      String connectionId, int id, String sql, DataStore dataStore) {
    return new StatementHolder(connectionId, id, prepare(sql, dataStore), dataStore);
  }

  public static StatementHolder empty(String connectionId, int id, DataStore dataStore) {
    return new StatementHolder(connectionId, id, new Prepared(null, null), dataStore);
  }

  public ExecutionResult result() {
    return result;
  }

  private ExecutionResult execute(Prepared prepared, List<TypedValue> params) {
    List<Object> localParams =
        params.stream().map(TypedValue::toLocal).collect(Collectors.toList());
    Iterable<Object> rows = prepared.query().execute(localParams);
    ExecutionResult r = new ExecutionResult(prepared, rows.iterator());
    result = r;
    return r;
  }

  public ExecutionResult execute(List<TypedValue> params) {
    return execute(prepared, params);
  }

  public ExecutionResult execute(String sql, List<TypedValue> params) {
    return execute(prepare(sql, dataStore), params);
  }

  private static Prepared prepare(String sql, DataStore dataStore) {
    PreparedSqlQuery prepared;
    try {
      prepared = queryPlanner.prepare(sql, dataStore, null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    Meta.Signature sig = makeSignature(prepared, sql);
    return new Prepared(prepared, sig);
  }

  private static Meta.Signature makeSignature(PreparedSqlQuery prepared, String sql) {
    RelDataType recordType = prepared.getResultType();
    List<RelDataTypeField> fields = recordType.getFieldList();
    List<ColumnMetaData> columnMetaData = new ArrayList<>(fields.size());
    int idx = 0;
    for (RelDataTypeField f : fields) {
      RelDataType relType = f.getType();
      int nullable =
          relType.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
      int displaySize = Integer.MAX_VALUE;
      String label = f.getName();
      String name = f.getName();
      String schemaName = "unknown"; // TODO: schema name in result sets
      String tableName = ""; // TODO: table name in result sets when selecting from a single table
      String catalogue = "";
      int precision = coercePrecision(relType);
      int scale = coerceScale(relType);

      JavaTypeFactory typeFactory = prepared.getTypeFactory();
      ColumnMetaData.AvaticaType type = buildAvaticaType(typeFactory, relType);

      columnMetaData.add(
          new ColumnMetaData(
              idx++,
              false,
              false,
              false,
              false,
              nullable,
              false,
              displaySize,
              label,
              name,
              schemaName,
              precision,
              scale,
              tableName,
              catalogue,
              type,
              true,
              false,
              false,
              type.columnClassName()));
    }

    Meta.CursorFactory cursorFactory = Meta.CursorFactory.ARRAY;
    Meta.StatementType type =
        prepared.isDml() ? Meta.StatementType.IS_DML : Meta.StatementType.SELECT;

    List<AvaticaParameter> parameters =
        prepared.getParametersType().getFieldList().stream()
            .map(
                f -> {
                  RelDataType fieldType = f.getType();
                  ColumnMetaData.ScalarType jdbcType =
                      buildAvaticaType(prepared.getTypeFactory(), fieldType);
                  return new AvaticaParameter(
                      false,
                      coercePrecision(fieldType),
                      coerceScale(fieldType),
                      getJdbcOrdinal(fieldType),
                      getJdbcTypeName(fieldType),
                      jdbcType.columnClassName(),
                      f.getName());
                })
            .collect(Collectors.toList());

    return new Meta.Signature(
        columnMetaData, sql, parameters, Collections.emptyMap(), cursorFactory, type);
  }

  private static int coercePrecision(RelDataType type) {
    return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision();
  }

  private static int coerceScale(RelDataType type) {
    return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED ? 0 : type.getScale();
  }

  private static String getJdbcTypeName(RelDataType type) {
    return type.getSqlTypeName().getName();
  }

  private static int getJdbcOrdinal(RelDataType type) {
    return type.getSqlTypeName().getJdbcOrdinal();
  }

  private static ColumnMetaData.ScalarType buildAvaticaType(
      JavaTypeFactory typeFactory, RelDataType type) {
    Type clazz = typeFactory.getJavaClass(type);
    ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
    return ColumnMetaData.scalar(getJdbcOrdinal(type), getJdbcTypeName(type), rep);
  }

  public static class Prepared {
    private final PreparedSqlQuery query;
    private final Meta.Signature signature;

    private Prepared(PreparedSqlQuery query, Meta.Signature signature) {
      this.query = query;
      this.signature = signature;
    }

    public PreparedSqlQuery query() {
      return query;
    }

    public Meta.Signature signature() {
      return signature;
    }
  }

  public static class ExecutionResult {
    private final Prepared prepared;
    private final Iterator<Object> rows;
    private final AtomicLong offset = new AtomicLong();

    public ExecutionResult(Prepared prepared, Iterator<Object> rows) {
      this.prepared = prepared;
      this.rows = rows;
    }

    public Prepared prepared() {
      return prepared;
    }

    public boolean isUpdate() {
      return prepared.signature.statementType.canUpdate();
    }

    public long updateCount() {
      return ((Number) rows.next()).longValue();
    }

    public Frame fetch(long requestedOffset, int maxRows) {
      long startOffset = offset.get();
      long currentOffset = startOffset;

      if (requestedOffset < currentOffset) {
        throw new IllegalStateException(
            String.format(
                "Requested offset (%d) is below current mark (%d)",
                requestedOffset, currentOffset));
      }

      ImmutableList.Builder<Object> resultRows = ImmutableList.builder();

      while (rows.hasNext()) {
        if (maxRows <= 0) {
          break;
        }

        Object row = rows.next();

        if (currentOffset++ < requestedOffset) {
          continue; // skip current row
        }

        if (row.getClass().isArray()) {
          resultRows.add(row);
        } else {
          resultRows.add((Object) (new Object[] {row}));
        }

        maxRows--;
      }

      if (!offset.compareAndSet(startOffset, currentOffset)) {
        throw new IllegalStateException("Concurrent fetch from the same result set");
      }

      return Frame.create(0, !rows.hasNext(), resultRows.build());
    }
  }
}
