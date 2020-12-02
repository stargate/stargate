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

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.calcite.runtime.SqlFunctions;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.ByteConverter;

public class PGUtils {

  private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("GMT");

  private static final ThreadLocal<PGUtils> threadLocal = ThreadLocal.withInitial(PGUtils::new);

  private final Calendar calendar = Calendar.getInstance(TIME_ZONE);
  private final TimestampUtils pgUtils;

  private PGUtils() {
    pgUtils = new TimestampUtils(true, () -> TIME_ZONE);
  }

  public static String asString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static Long timestamp(String value) {
    PGUtils utils = threadLocal.get();
    try {
      Timestamp timestamp = utils.pgUtils.toTimestamp(utils.calendar, value);
      return SqlFunctions.toLong(timestamp);
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Long timestamp(byte[] value) {
    PGUtils utils = threadLocal.get();
    try {
      Timestamp timestamp = utils.pgUtils.toTimestampBin(TIME_ZONE, value, true);
      return SqlFunctions.toLong(timestamp);
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static Integer time(LocalTime value) {
    Time time = Time.valueOf(value);
    return SqlFunctions.toInt(time);
  }

  public static Integer time(String value) {
    PGUtils utils = threadLocal.get();
    try {
      LocalDateTime date = utils.pgUtils.toLocalDateTime(value);
      return time(date.toLocalTime());
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Integer time(byte[] value) {
    PGUtils utils = threadLocal.get();
    try {
      LocalDateTime date = utils.pgUtils.toLocalDateTimeBin(value);
      return time(date.toLocalTime());
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static Integer date(LocalDateTime value) {
    LocalDate localDate = value.toLocalDate();
    return SqlFunctions.toInt(java.sql.Date.valueOf(localDate));
  }

  public static Integer date(String value) {
    PGUtils utils = threadLocal.get();
    try {
      LocalDateTime date = utils.pgUtils.toLocalDateTime(value);
      return date(date);
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Integer date(byte[] value) {
    PGUtils utils = threadLocal.get();
    try {
      LocalDateTime date = utils.pgUtils.toLocalDateTimeBin(value);
      return date(date);
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static byte int2byte(byte[] value) {
    return (byte) ByteConverter.int2(value, 0);
  }

  public static short int2(byte[] value) {
    return ByteConverter.int2(value, 0);
  }

  public static int int4(byte[] value) {
    return ByteConverter.int4(value, 0);
  }

  public static long int8(byte[] value) {
    return ByteConverter.int8(value, 0);
  }

  public static double float8(byte[] value) {
    return ByteConverter.float8(value, 0);
  }

  public static Number numeric(byte[] value) {
    return ByteConverter.numeric(value);
  }

  public static boolean bool(byte[] value) {
    return ByteConverter.bool(value, 0);
  }
}
