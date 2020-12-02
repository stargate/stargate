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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.function.Function;
import org.postgresql.core.Oid;

public enum PGType {
  Bool(Oid.BOOL, 1, Boolean::parseBoolean, PGUtils::bool),
  Date(Oid.DATE, 4, PGUtils::date, PGUtils::date),
  Time(Oid.TIME, 8, PGUtils::time, PGUtils::time),
  Timestamp(Oid.TIMESTAMPTZ, 8, PGUtils::timestamp, PGUtils::timestamp),
  Float8(Oid.FLOAT8, 8, Float::parseFloat, PGUtils::float8),
  Int2tiny(Oid.INT2, 2, Byte::parseByte, PGUtils::int2byte), // Note: same OID as Int2
  Int2(Oid.INT2, 2, Short::parseShort, PGUtils::int2),
  Int4(Oid.INT4, 4, Integer::parseInt, PGUtils::int4),
  Int8(Oid.INT8, 8, Long::parseLong, PGUtils::int8),
  Numeric(Oid.NUMERIC, -1, BigDecimal::new, PGUtils::numeric),
  Varchar(Oid.VARCHAR, -1, s -> s, PGUtils::asString);

  private final int oid;
  private final int length;
  private final Function<String, Object> textParser;
  private final Function<byte[], Object> binParser;

  PGType(
      int oid,
      int length,
      Function<String, Object> textParser,
      Function<byte[], Object> binParser) {
    this.oid = oid;
    this.length = length;
    this.textParser = textParser;
    this.binParser = binParser;
  }

  static PGType of(int oid) {
    return Arrays.stream(values())
        .filter(t -> t.oid == oid)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unsupported type OID: " + oid));
  }

  public int oid() {
    return oid;
  }

  public int length() {
    return length;
  }

  public Object parse(byte[] value, int format) {
    if (value == null) {
      return null;
    }

    // Cf. https://www.postgresql.org/docs/13/protocol-overview.html#PROTOCOL-FORMAT-CODES
    switch (format) {
      case 0:
        return textParser.apply(PGUtils.asString(value));

      case 1:
        return binParser.apply(value);
    }

    throw new IllegalArgumentException("Unsupported format " + format + " for type " + this);
  }
}
