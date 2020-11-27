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
package io.stargate.api.sql.server.postgres;

import io.reactivex.Flowable;
import io.stargate.api.sql.plan.PreparedSqlQuery;
import io.stargate.api.sql.server.postgres.msg.CommandComplete;
import io.stargate.api.sql.server.postgres.msg.DataRow;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

public class Portal {

  private final Statement statement;
  private final List<FieldInfo> fields;

  public Portal(Statement statement, int[] resultFormatCodes) {
    this.statement = statement;
    this.fields = fields(statement, resultFormatCodes);
  }

  public Flowable<PGServerMessage> execute(Connection connection) {
    Iterable<Object> rows = statement.execute(connection);
    AtomicLong count = new AtomicLong();
    return Flowable.fromIterable(rows)
        .map(
            row -> {
              count.incrementAndGet();
              return toDataRow(row);
            })
        .concatWith(Flowable.defer(() -> Flowable.just(CommandComplete.forSelect(count.get()))));
  }

  private PGServerMessage toDataRow(Object row) {
    DataRow result = DataRow.create();

    if (row instanceof Object[]) {
      Object[] values = (Object[]) row;
      int idx = 0;
      for (FieldInfo field : fields) {
        result.add(field, toPGValue(field, values[idx++]));
      }
    } else {
      FieldInfo field = fields.get(0);
      result.add(field, toPGValue(field, row));
    }

    return result;
  }

  public List<FieldInfo> fields() {
    return fields;
  }

  private static List<FieldInfo> fields(Statement statement, int[] resultFormatCodes) {
    PreparedSqlQuery prepared = statement.prepared();
    if (prepared == null) {
      return Collections.emptyList();
    }

    RelDataType type = prepared.getResultType();

    List<RelDataTypeField> fieldList = type.getFieldList();
    List<FieldInfo> fields = new ArrayList<>(fieldList.size());
    int idx = 0;
    for (RelDataTypeField ft : fieldList) {
      fields.add(toFieldInfo(ft, idx++, resultFormatCodes));
    }

    return fields;
  }

  private static FieldInfo toFieldInfo(RelDataTypeField type, int pos, int[] resultFormatCodes) {
    FieldFormat format = FieldFormat.TEXT;
    if (resultFormatCodes.length == 1) {
      format = FieldFormat.from(resultFormatCodes[0]);
    } else if (resultFormatCodes.length > 1) {
      if (pos >= resultFormatCodes.length) {
        throw new IllegalStateException(
            "Bound fields do not define format code for result set "
                + "column number: "
                + (pos + 1)); // follow JDBC column indexing convention
      }

      format = FieldFormat.from(resultFormatCodes[pos]);
    }

    PGType pgType = toPGType(type.getType());
    return new FieldInfo(type.getName(), format, pgType);
  }

  private static Object toPGValue(FieldInfo field, Object value) {
    switch (field.getType()) {
      case Timestamp:
        return SqlFunctions.internalToTimestamp((Long) value);
      case Time:
        return SqlFunctions.internalToTime((Integer) value);
      case Date:
        return SqlFunctions.internalToDate((Integer) value);
    }

    return value;
  }

  private static PGType toPGType(RelDataType type) {
    SqlTypeName sqlType = type.getSqlTypeName();
    switch (sqlType) {
      case VARCHAR:
        return PGType.Varchar;
      case INTEGER:
        return PGType.Int4;
      case BIGINT:
        return PGType.Int8;
      case BOOLEAN:
        return PGType.Bool;
      case DATE:
        return PGType.Date;
      case TIME:
        return PGType.Time;
      case TIMESTAMP:
        return PGType.Timestamp;
      case TINYINT:
        // Note: the 1-byte char type is treated as a String by the PostgreSQL JDBC driver
        return PGType.Int2;
      case DECIMAL:
        return PGType.Numeric;
      case DOUBLE:
        return PGType.Float8;
      case SMALLINT:
        return PGType.Int2;
      default:
        throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
    }
  }
}
