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
package io.stargate.api.sql.schema;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import io.stargate.db.schema.Column;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

public class TypeUtils {
  public static SqlTypeName toCalciteType(Column.ColumnType type) {
    if (type == null) {
      throw new IllegalArgumentException("Null column type");
    }

    // TODO: support other types
    if (type.isComplexType()) {
      return SqlTypeName.OTHER;
    }

    switch (type.rawType()) {
      case Ascii:
      case Varchar:
      case Text:
      case Uuid:
      case Timeuuid:
      case Duration:
      case Inet:
        return SqlTypeName.VARCHAR;
      case Bigint:
      case Counter:
        return SqlTypeName.BIGINT;
      case Blob:
        return SqlTypeName.BINARY;
      case Boolean:
        return SqlTypeName.BOOLEAN;
      case Date:
        return SqlTypeName.DATE;
      case Decimal:
        return SqlTypeName.DECIMAL;
      case Double:
        return SqlTypeName.DOUBLE;
      case Float:
        return SqlTypeName.FLOAT;
      case Int:
        return SqlTypeName.INTEGER;
      case Smallint:
        return SqlTypeName.SMALLINT;
      case Tinyint:
        return SqlTypeName.TINYINT;
      case Time:
        return SqlTypeName.TIME;
      case Timestamp:
        return SqlTypeName.TIMESTAMP;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.rawType());
    }
  }

  public static Column.ColumnType fromSqlType(SqlTypeName sqlType) {
    switch (sqlType) {
      case BIGINT:
        return Column.Type.Bigint;
      case BINARY:
        return Column.Type.Blob;
      case BOOLEAN:
        return Column.Type.Boolean;
      case DATE:
        return Column.Type.Date;
      case DECIMAL:
        return Column.Type.Decimal;
      case DOUBLE:
        return Column.Type.Double;
      case FLOAT:
        return Column.Type.Float;
      case INTEGER:
        return Column.Type.Int;
      case SMALLINT:
        return Column.Type.Smallint;
      case TINYINT:
        return Column.Type.Tinyint;
      default:
        return Column.Type.Text;
    }
  }

  public static Object toJdbcValue(Object driverValue, Column.ColumnType type) {
    if (type.isUserDefined() || type.isParameterized()) {
      throw new IllegalArgumentException(
          "Unsupported type: " + type.name()); // TODO: support collections
    }

    switch (type.rawType()) {
      case Blob:
        ByteBuffer buffer = (ByteBuffer) driverValue;
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new ByteString(bytes);
      case Varint:
        BigInteger i = (BigInteger) driverValue;
        return new BigDecimal(i);
      case Date:
        LocalDate ld = (LocalDate) driverValue;
        return java.sql.Date.valueOf(ld);
      case Time:
        LocalTime lt = (LocalTime) driverValue;
        return java.sql.Time.valueOf(lt);
      case Timestamp:
        Instant ts = (Instant) driverValue;
        return java.sql.Timestamp.from(ts);
      case Duration:
        CqlDuration raw = (CqlDuration) driverValue;
        return toIsoDuration(raw);
      case Inet:
        InetAddress a = (InetAddress) driverValue;
        return a.getHostAddress();
      case Timeuuid:
      case Uuid:
        UUID uuid = (UUID) driverValue;
        return uuid.toString();
      default:
        return driverValue;
    }
  }

  public static Object jdbcToDriverValue(Object jdbcValue, Column.ColumnType type) {
    if (type.isUserDefined() || type.isParameterized()) {
      throw new IllegalArgumentException(
          "Unsupported type: " + type.name()); // TODO: support collections
    }

    switch (type.rawType()) {
      case Varint:
        BigDecimal i = (BigDecimal) jdbcValue;
        return i.toBigInteger();
      case Date:
        if (jdbcValue instanceof LocalDate) {
          return jdbcValue;
        } else {
          return ((Date) jdbcValue).toLocalDate();
        }
      case Time:
        if (jdbcValue instanceof LocalTime) {
          return jdbcValue;
        } else {
          return ((Time) jdbcValue).toLocalTime();
        }
      case Timestamp:
        return ((Timestamp) jdbcValue).toInstant();
      case Duration:
        return CqlDuration.from((String) jdbcValue);
      case Inet:
        try {
          return InetAddress.getByName((String) jdbcValue);
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException(e);
        }
      case Timeuuid:
      case Uuid:
        return UUID.fromString((String) jdbcValue);
      default:
        return jdbcValue;
    }
  }

  public static Object toCalciteValue(Object driverValue, Column.ColumnType type) {
    if (type.isUserDefined() || type.isParameterized()) {
      throw new IllegalArgumentException(
          "Unsupported type: " + type.name()); // TODO: support collections
    }

    Object jdbcValue = toJdbcValue(driverValue, type);

    switch (type.rawType()) {
      case Date:
        return SqlFunctions.toInt((java.sql.Date) jdbcValue);
      case Time:
        return SqlFunctions.toInt((java.sql.Time) jdbcValue);
      case Timestamp:
        return SqlFunctions.toLong(jdbcValue);
      default:
        return jdbcValue;
    }
  }

  public static Object toDriverValue(Object calciteValue, Column.ColumnType type) {
    if (type.isUserDefined() || type.isParameterized()) {
      throw new IllegalArgumentException(
          "Unsupported type: " + type.name()); // TODO: support collections
    }

    Object jdbcValue;

    switch (type.rawType()) {
      case Date:
        jdbcValue = SqlFunctions.internalToDate((Integer) calciteValue);
        break;
      case Time:
        jdbcValue = SqlFunctions.internalToTime((Integer) calciteValue);
        break;
      case Timestamp:
        jdbcValue = SqlFunctions.internalToTimestamp((Long) calciteValue);
        break;
      default:
        jdbcValue = calciteValue;
    }

    return jdbcToDriverValue(jdbcValue, type);
  }

  public static String toIsoDuration(CqlDuration duration) {
    // ISO-8601
    return "P"
        + duration.getMonths()
        + 'M'
        + duration.getDays()
        + 'D'
        + 'T'
        + TimeUnit.NANOSECONDS.toSeconds(duration.getNanoseconds())
        + 'S';
  }
}
