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
import io.stargate.api.sql.server.postgres.msg.Bind;
import io.stargate.api.sql.server.postgres.msg.DataRow;
import io.stargate.api.sql.server.postgres.msg.NoData;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import io.stargate.api.sql.server.postgres.msg.RowDescription;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class Portal {

  private final List<FieldInfo> fields;
  private final List<Object> parameters;

  public Portal(Statement statement, Bind bind) {
    this.fields = fields(statement, bind.getResultFormatCodes());
    this.parameters = parameters(statement, bind);
  }

  protected abstract boolean hasResultSet();

  protected List<Object> parameters() {
    return parameters;
  }

  public abstract Flowable<PGServerMessage> execute(Connection connection);

  protected PGServerMessage toDataRow(Object row) {
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
        return PGType.Int2tiny;
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

  private static List<Object> parameters(Statement statement, Bind bind) {
    PreparedSqlQuery prepared = statement.prepared();
    if (prepared == null) {
      return Collections.emptyList();
    }

    bind.numberOfParameters();
    List<RelDataTypeField> fields = prepared.getParametersType().getFieldList();

    if (fields.size() != bind.numberOfParameters()) {
      throw new IllegalArgumentException(
          String.format(
              "Statement has %d parameters, but %d are bound",
              fields.size(), bind.numberOfParameters()));
    }

    List<Object> values = new ArrayList<>(fields.size());

    int idx = 0;
    for (RelDataTypeField field : fields) {
      int format = bind.parameterFormat(idx);
      byte[] bytes = bind.parameterBytes(idx);
      idx++;

      PGType pgType = toPGType(field.getType());
      values.add(pgType.parse(bytes, format));
    }

    return values;
  }

  public Flowable<PGServerMessage> describeSimple() {
    if (hasResultSet()) {
      return Flowable.just(RowDescription.from(this));
    } else {
      return Flowable.empty();
    }
  }

  public PGServerMessage describe() {
    if (hasResultSet()) {
      return RowDescription.from(this);
    } else {
      return NoData.instance();
    }
  }
}
