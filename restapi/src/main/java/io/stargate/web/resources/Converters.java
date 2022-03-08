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
package io.stargate.web.resources;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Modification.Operation;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.Value;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ParameterizedType.TupleType;
import io.stargate.db.schema.ReservedKeywords;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.restapi.models.ClusteringExpression;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.TableOptions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Converters {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    // For Java 8 date/time types we need, as per [stargate#1680]:
    OBJECT_MAPPER.registerModule(new JavaTimeModule());
    // and by default date/time values written as numbers but we prefer ISO-8601:
    OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
  private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
  private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");

  public static Map<String, Object> row2Map(final Row row) {
    final Map<String, Object> map = new HashMap<>(row.columns().size());
    for (final Column column : row.columns()) {
      map.put(column.name(), toJsonValue(row.getObject(column.name())));
    }
    return map;
  }

  /**
   * Legacy implementation of {@link #row2Map} for the v1 API.
   *
   * <p>Note that passing column values directly to the object mapper presents a number of issues:
   *
   * <ul>
   *   <li>Longs and BigIntegers are converted to JSON numbers, which can lose range.
   *   <li>BigDecimals are converted to JSON numbers, which can lose precision.
   *   <li>Map keys are always serialized into strings.
   *   <li>UDT and tuple values leak all the internal fields of {@link UdtValue} and {@link
   *       TupleValue}.
   * </ul>
   *
   * But it was decided to keep v1 as-is for backward compatibility purposes.
   */
  public static Map<String, Object> row2MapV1(final Row row) {
    final Map<String, Object> map = new HashMap<>(row.columns().size());
    for (final Column column : row.columns()) {
      Object value = row.getObject(column.name());
      if (value instanceof Object[]) {
        value = Arrays.asList((Object[]) value);
      }
      map.put(column.name(), value);
    }
    return map;
  }

  @VisibleForTesting
  static Object toJsonValue(final Object cqlValue) {
    if (cqlValue instanceof UUID
        || cqlValue instanceof CqlDuration
        // Large numbers can cause JSON interoperability issues, for example Javascript only handles
        // integers in the range [-(2**53)+1, (2**53)-1].
        || cqlValue instanceof Long
        || cqlValue instanceof BigInteger
        || cqlValue instanceof BigDecimal) {
      return cqlValue.toString();
    }
    if (cqlValue instanceof InetAddress) {
      return ((InetAddress) cqlValue).getHostAddress();
    }
    if (cqlValue instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> cqlCollection = (Collection<Object>) cqlValue;
      return cqlCollection.stream().map(Converters::toJsonValue).collect(Collectors.toList());
    }
    if (cqlValue instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> cqlMap = (Map<Object, Object>) cqlValue;
      return cqlMap.entrySet().stream()
          .map(
              entry ->
                  ImmutableMap.of(
                      "key", toJsonValue(entry.getKey()), "value", toJsonValue(entry.getValue())))
          .collect(Collectors.toList());
    }
    if (cqlValue instanceof UdtValue) {
      UdtValue udtValue = (UdtValue) cqlValue;
      com.datastax.oss.driver.api.core.type.UserDefinedType udtType = udtValue.getType();
      Map<String, Object> jsonObject = Maps.newLinkedHashMapWithExpectedSize(udtValue.size());
      for (int i = 0; i < udtValue.size(); i++) {
        CqlIdentifier fieldName = udtType.getFieldNames().get(i);
        jsonObject.put(fieldName.asInternal(), toJsonValue(udtValue.getObject(fieldName)));
      }
      return jsonObject;
    }
    if (cqlValue instanceof TupleValue) {
      TupleValue tupleValue = (TupleValue) cqlValue;
      List<Object> jsonArray = Lists.newArrayListWithCapacity(tupleValue.size());
      for (int i = 0; i < tupleValue.size(); i++) {
        jsonArray.add(toJsonValue(tupleValue.getObject(i)));
      }
      return jsonArray;
    }

    // This covers null, booleans, strings, the remaining number types, blobs (which Jackson already
    // converts to base64 natively), and time types (which are handled by registering JavaTimeModule
    // with the object mapper -- see Server.java).
    return cqlValue;
  }

  public static BuiltCondition idToWhere(String val, String name, AbstractTable tableData) {
    Column column = tableData.column(name);
    if (column == null) {
      throw new IllegalArgumentException(String.format("Unknown field name '%s'.", name));
    }
    Column.ColumnType type = column.type();
    Object value = val;

    if (type != null) {
      value = toCqlValue(type, val);
    }

    return BuiltCondition.of(name, Predicate.EQ, value);
  }

  public static ValueModifier colToValue(String name, Object value, AbstractTable tableData) {
    Column column = tableData.column(name);
    if (column == null) {
      throw new IllegalArgumentException(String.format("Unknown field name '%s'.", name));
    }
    Column.ColumnType type = column.type();
    Object valueObj = value;

    if (type != null) {
      valueObj = toCqlValue(type, value);

      if (type == Type.Counter) {
        // only using increment to keep this simple since decrement is just adding a negative
        return ValueModifier.of(name, Value.of(valueObj), Operation.INCREMENT);
      }
    }

    return ValueModifier.set(name, valueObj);
  }

  /** Converts an incoming JSON value into a Java type suitable for the given column type. */
  @SuppressWarnings("unchecked")
  public static Object toCqlValue(Column.ColumnType type, Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    if (jsonValue instanceof String) {
      return toCqlValue(type, (String) jsonValue);
    }

    switch (type.rawType()) {
      case Text:
      case Ascii:
      case Uuid:
      case Timeuuid:
      case Blob:
      case Inet:
      case Duration:
        throw new IllegalArgumentException(
            String.format("Invalid %s value '%s': expected a string", type.rawType(), jsonValue));
      case Boolean:
        if (jsonValue instanceof Boolean) {
          return jsonValue;
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Boolean value '%s': expected a boolean or a string", jsonValue));
        }
      case Tinyint:
        return toCqlInt(
            type,
            jsonValue,
            BigInteger::shortValueExact,
            Number::byteValue,
            Byte.MIN_VALUE,
            Byte.MAX_VALUE);
      case Smallint:
        return toCqlInt(
            type,
            jsonValue,
            BigInteger::shortValueExact,
            Number::shortValue,
            Short.MIN_VALUE,
            Short.MAX_VALUE);
      case Int:
        return toCqlInt(
            type,
            jsonValue,
            BigInteger::intValueExact,
            Number::intValue,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE);
      case Bigint:
      case Counter:
        return toCqlInt(
            type,
            jsonValue,
            BigInteger::longValueExact,
            Number::longValue,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      case Varint:
        if (jsonValue instanceof BigInteger) {
          return jsonValue;
        } else if (jsonValue instanceof Integer || jsonValue instanceof Long) {
          return BigInteger.valueOf(((Number) jsonValue).longValue());
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Varint value '%s': expected an integer or a string", jsonValue));
        }
      case Float:
        if (jsonValue instanceof Number) {
          return ((Number) jsonValue).floatValue();
        } else {
          throw new IllegalArgumentException(
              String.format("Invalid Float value '%s': expected a number or a string", jsonValue));
        }
      case Double:
        if (jsonValue instanceof Number) {
          return ((Number) jsonValue).doubleValue();
        } else {
          throw new IllegalArgumentException(
              String.format("Invalid Double value '%s': expected a number or a string", jsonValue));
        }
      case Decimal:
        if (jsonValue instanceof BigDecimal) {
          return jsonValue;
        } else if (jsonValue instanceof Number) {
          return BigDecimal.valueOf(((Number) jsonValue).doubleValue());
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Decimal value '%s': expected a number or a string", jsonValue));
        }
      case Date:
        if (jsonValue instanceof Integer || jsonValue instanceof Long) {
          return EPOCH.plusDays(cqlDateToDaysSinceEpoch(((Number) jsonValue).longValue()));
        } else {
          throw new IllegalArgumentException(
              String.format("Invalid Date value '%s': expected an integer or a string", jsonValue));
        }
      case Time:
        if (jsonValue instanceof Integer || jsonValue instanceof Long) {
          return LocalTime.ofNanoOfDay(((Number) jsonValue).longValue());
        } else {
          throw new IllegalArgumentException(
              String.format("Invalid Time value '%s': expected an integer or a string", jsonValue));
        }
      case Timestamp:
        if (jsonValue instanceof Integer || jsonValue instanceof Long) {
          return Instant.ofEpochMilli(((Number) jsonValue).longValue());
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Timestamp value '%s': expected an integer or a string", jsonValue));
        }
      case List:
        if (jsonValue instanceof List) {
          return toCqlCollection(
              type, (List<Object>) jsonValue, ArrayList::new, Collections.emptyList());
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid List value '%s': expected a JSON array or a string", jsonValue));
        }
      case Set:
        if (jsonValue instanceof List) {
          return toCqlCollection(
              type, (List<Object>) jsonValue, LinkedHashSet::new, Collections.emptySet());
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Set value '%s': expected a JSON array or a string", jsonValue));
        }
      case Map:
        if (jsonValue instanceof List) {
          return toCqlMap(type, (List<Object>) jsonValue);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Map value '%s': expected a JSON array of key/value objects, or a string",
                  jsonValue));
        }
      case Tuple:
        if (jsonValue instanceof List) {
          return toCqlTuple((TupleType) type, (List<Object>) jsonValue);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid Tuple value '%s': expected a JSON array or a string", jsonValue));
        }
      case UDT:
        if (jsonValue instanceof Map) {
          return toCqlUdt((UserDefinedType) type, (Map<String, Object>) jsonValue);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid UDT value '%s': expected a JSON object or a string", jsonValue));
        }
      default:
        throw new AssertionError("Unsupported data type: " + type.rawType());
    }
  }

  private static <I> I toCqlInt(
      Column.ColumnType type,
      Object jsonValue,
      Function<BigInteger, I> fromBigIntegerExact,
      Function<Number, I> fromNumber,
      long min,
      long max) {
    if (jsonValue instanceof BigInteger) {
      try {
        return fromBigIntegerExact.apply((BigInteger) jsonValue);
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            String.format("Invalid %s value '%s': out of range", type.rawType(), jsonValue));
      }
    }
    if (jsonValue instanceof Integer || jsonValue instanceof Long) {
      Number number = (Number) jsonValue;
      long longValue = number.longValue();
      if (longValue < min || longValue > max) {
        throw new IllegalArgumentException(
            String.format("Invalid %s value '%s': out of range", type.rawType(), jsonValue));
      }
      return fromNumber.apply(number);
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid %s value '%s': expected an integer or a string", type.rawType(), jsonValue));
  }

  private static <C extends Collection<Object>> C toCqlCollection(
      Column.ColumnType type, List<Object> jsonValues, Supplier<C> newInstance, C empty) {

    if (jsonValues.isEmpty()) {
      return empty;
    }
    Column.ColumnType elementType = type.parameters().get(0);
    C result = newInstance.get();
    int index = 0;
    for (Object jsonValue : jsonValues) {
      try {
        result.add(toCqlValue(elementType, jsonValue));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "%s (at index %d of %s '%s')", e.getMessage(), index, type.rawType(), jsonValues));
      }
      index += 1;
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  private static Map<Object, Object> toCqlMap(Column.ColumnType type, List<Object> jsonEntries) {
    if (jsonEntries.isEmpty()) {
      return Collections.emptyMap();
    }
    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);
    LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
    int index = 0;
    for (Object jsonValue : jsonEntries) {
      if (!(jsonValue instanceof Map)
          || !((Map) jsonValue).containsKey("key")
          || !((Map) jsonValue).containsKey("value")) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid Map value '%s': inner elements must be objects with two fields 'key' and 'value'",
                jsonEntries));
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> jsonEntry = (Map<String, Object>) jsonValue;
      Object key, value;
      try {
        key = toCqlValue(keyType, jsonEntry.get("key"));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("%s (key at index %d of Map '%s')", e.getMessage(), index, jsonEntries));
      }
      try {
        value = toCqlValue(valueType, jsonEntry.get("value"));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "%s (value at index %d of Map '%s')", e.getMessage(), index, jsonEntries));
      }
      result.put(key, toCqlValue(valueType, value));
      index += 1;
    }
    return result;
  }

  private static TupleValue toCqlTuple(TupleType type, List<Object> jsonValues) {
    List<Object> fields = Lists.newArrayListWithCapacity(jsonValues.size());
    int index = 0;
    for (Object jsonValue : jsonValues) {
      try {
        fields.add(toCqlValue(type.parameters().get(index), jsonValue));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "%s (field at index %d of Tuple '%s')", e.getMessage(), index, jsonValues));
      }
      index += 1;
    }
    return type.create(fields.toArray());
  }

  @SuppressWarnings("unchecked")
  private static UdtValue toCqlUdt(UserDefinedType type, Map<String, Object> jsonObject) {
    UdtValue udtValue = type.create();
    for (Map.Entry<String, Object> jsonEntry : jsonObject.entrySet()) {
      String fieldId = jsonEntry.getKey();
      Object jsonFieldValue = jsonEntry.getValue();
      if (!type.columnMap().containsKey(fieldId)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': unknown field name \"%s\"", jsonObject, fieldId));
      }
      Column.ColumnType fieldType = type.fieldType(fieldId);
      try {
        udtValue = udtValue.set(fieldId, toCqlValue(fieldType, jsonFieldValue), fieldType.codec());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("%s (field '%s' of UDT '%s')", e.getMessage(), fieldId, jsonObject));
      }
    }
    return udtValue;
  }

  private static int cqlDateToDaysSinceEpoch(long raw) {
    if (raw < 0 || raw > MAX_CQL_LONG_VALUE)
      throw new IllegalArgumentException(
          String.format(
              "Invalid Date value '%d': numeric literals must be between 0 and %d",
              raw, MAX_CQL_LONG_VALUE));
    return (int) (raw - EPOCH_AS_CQL_LONG);
  }

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
  private static final long MAX_CQL_LONG_VALUE = ((1L << 32) - 1);
  private static final long EPOCH_AS_CQL_LONG = (1L << 31);

  /**
   * Converts an incoming JSON string value into a Java type suitable for the given column type.
   *
   * <p>This method exists separately from {@link #toCqlValue(Column.ColumnType, Object)} for the v1
   * REST API, which only allows strings.
   */
  public static Object toCqlValue(Column.ColumnType type, String value) {
    value = value.trim();
    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1).replaceAll("''", "'");
    }

    Column.Type rawType = type.rawType();
    // Handle complex types separately because they already handle parsing errors:
    if (rawType == Column.Type.List) {
      return toCqlCollection(type, value, ArrayList::new, Collections.emptyList(), '[', ']');
    }
    if (rawType == Column.Type.Set) {
      return toCqlCollection(type, value, LinkedHashSet::new, Collections.emptySet(), '{', '}');
    }
    if (rawType == Column.Type.Map) {
      return toCqlMap(type, value);
    }
    if (rawType == Column.Type.Tuple) {
      return toCqlTuple((TupleType) type, value);
    }

    if (rawType == Column.Type.UDT) {
      return toCqlUdt((UserDefinedType) type, value);
    }

    if (rawType == Column.Type.Text || rawType == Column.Type.Ascii) {
      return value;
    }

    if (rawType == Column.Type.Boolean) {
      // Boolean.parseBoolean returns false for anything not equal to "true"
      if (value.equalsIgnoreCase("true")) {
        return true;
      } else if (value.equalsIgnoreCase("false")) {
        return false;
      } else {
        throw new IllegalArgumentException(
            String.format("Invalid Boolean value '%s': cannot parse", value));
      }
    }

    try {
      if (rawType == Column.Type.Uuid || rawType == Column.Type.Timeuuid) {
        return UUID.fromString(value);
      }
      if (rawType == Column.Type.Blob) {
        return ByteBuffer.wrap(Base64.getDecoder().decode(value));
      }
      if (rawType == Column.Type.Duration) {
        return CqlDuration.from(value);
      }
      if (rawType == Column.Type.Tinyint) {
        return Byte.valueOf(value);
      }
      if (rawType == Column.Type.Smallint) {
        return Short.valueOf(value);
      }
      if (rawType == Column.Type.Int) {
        return Integer.valueOf(value);
      }
      if (rawType == Column.Type.Bigint || rawType == Column.Type.Counter) {
        return Long.valueOf(value);
      }
      if (rawType == Column.Type.Float) {
        return Float.parseFloat(value);
      }
      if (rawType == Column.Type.Double) {
        return Double.parseDouble(value);
      }
      if (rawType == Column.Type.Varint) {
        return new BigInteger(value);
      }
      if (rawType == Column.Type.Decimal) {
        return new BigDecimal(value);
      }
    } catch (IllegalArgumentException e) {
      // The exception's message doesn't really add information in any of those cases, use a generic
      // description:
      throw new IllegalArgumentException(
          String.format("Invalid %s value '%s': cannot parse", rawType, value));
    }

    if (rawType == Column.Type.Inet) {
      try {
        return InetAddress.getByName(value);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid Inet value " + e.getMessage());
      }
    }

    try {
      if (rawType == Column.Type.Date) {
        return LocalDate.parse(value);
      }
      if (rawType == Column.Type.Time) {
        return LocalTime.parse(value);
      }
      if (rawType == Column.Type.Timestamp) {
        return Instant.parse(value);
      }
    } catch (DateTimeParseException e) {
      // The original message is pretty detailed, keep it:
      throw new IllegalArgumentException(
          String.format("Invalid %s value: %s", rawType, e.getMessage()));
    }

    throw new AssertionError("Unsupported data type: " + rawType);
  }

  private static <C extends Collection<Object>> C toCqlCollection(
      Column.ColumnType type,
      String value,
      Supplier<C> newInstance,
      C empty,
      char openingBrace,
      char closingBrace) {
    int idx = ParseUtils.skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting '%c' but got EOF",
              type.rawType(), value, idx, openingBrace));
    }
    if (value.charAt(idx++) != openingBrace) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting '%s' but got '%c'",
              type.rawType(), value, idx, openingBrace, value.charAt(idx)));
    }
    idx = ParseUtils.skipSpaces(value, idx);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting element or '%c' but got EOF",
              type.rawType(), value, idx, closingBrace));
    }
    if (value.charAt(idx) == closingBrace) {
      return empty;
    }
    C collection = newInstance.get();
    Column.ColumnType elementType = type.parameters().get(0);
    while (idx < value.length()) {
      int n = ParseUtils.skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': invalid CQL value at character %d",
                type.rawType(), value, idx));
      }

      collection.add(toCqlValue(elementType, value.substring(idx, n)));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': at character %d expecting ',' or '%c' but got EOF",
                type.rawType(), value, idx, closingBrace));
      }
      if (value.charAt(idx) == closingBrace) {
        return collection;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': at character %d expecting ',' but got '%c'",
                type.rawType(), value, idx, value.charAt(idx)));
      }

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid %s value '%s': missing closing '%s'", type.rawType(), value, closingBrace));
  }

  private static Map<Object, Object> toCqlMap(Column.ColumnType type, String value) {
    int idx = ParseUtils.skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Map value '%s': at character %d expecting '{' but got EOF", value, idx));
    }
    if (value.charAt(idx++) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid map value '%s': at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx = ParseUtils.skipSpaces(value, idx);

    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Map value '%s': at character %d expecting element or '}' but got EOF",
              value, idx));
    }
    if (value.charAt(idx) == '}') {
      return Collections.emptyMap();
    }

    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);

    Map<Object, Object> map = new LinkedHashMap<>();
    while (idx < value.length()) {
      int n = ParseUtils.skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid map value '%s': invalid CQL value at character %d", value, idx));
      }

      Object k = toCqlValue(keyType, value.substring(idx, n));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ':' but got EOF", value, idx));
      }
      if (value.charAt(idx) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ':' but got '%c'",
                value, idx, value.charAt(idx)));
      }
      idx = ParseUtils.skipSpaces(value, ++idx);

      n = ParseUtils.skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid map value '%s': invalid CQL value at character %d", value, idx));
      }

      Object v = toCqlValue(valueType, value.substring(idx, n));
      idx = n;

      map.put(k, v);

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ',' or '}' but got EOF",
                value, idx));
      }
      if (value.charAt(idx) == '}') {
        return map;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));
      }

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Invalid map value '%s': missing closing '}'", value));
  }

  private static TupleValue toCqlTuple(TupleType type, String value) {
    List<Object> fields = new ArrayList<>();
    int length = value.length();

    int idx = ParseUtils.skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Tuple value '%s': at character %d expecting '(' but got EOF", value, idx));
    }
    if (value.charAt(idx) != '(') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid tuple value '%s': at character %d expecting '(' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx++;
    idx = ParseUtils.skipSpaces(value, idx);

    int fieldIndex = 0;
    while (idx < length) {
      if (value.charAt(idx) == ')') {
        idx = ParseUtils.skipSpaces(value, idx + 1);
        if (idx == length) {
          return type.create(fields.toArray());
        }
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at character %d expecting EOF or blank, but got \"%s\"",
                value, idx, value.substring(idx)));
      }
      int n = ParseUtils.skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': invalid CQL value at field %d (character %d)",
                value, fieldIndex, idx));
      }

      String fieldValue = value.substring(idx, n);
      try {
        fields.add(toCqlValue(type.parameters().get(fieldIndex), fieldValue));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': invalid CQL value at field %d (character %d): %s",
                value, fieldIndex, idx, e.getMessage()),
            e);
      }

      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at field %d (character %d) expecting ',' or ')', but got EOF",
                value, fieldIndex, idx));
      }
      if (value.charAt(idx) == ')') {
        continue;
      }
      if (value.charAt(idx) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at field %d (character %d) expecting ',' but got '%c'",
                value, fieldIndex, idx, value.charAt(idx)));
      }
      ++idx; // skip ','

      idx = ParseUtils.skipSpaces(value, idx);
      fieldIndex += 1;
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid tuple value '%s': at field %d (character %d) expecting CQL value or ')', got EOF",
            value, fieldIndex, idx));
  }

  @SuppressWarnings("unchecked")
  private static UdtValue toCqlUdt(UserDefinedType type, String value) {
    UdtValue udtValue = type.create();
    int length = value.length();

    int idx = ParseUtils.skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting '{' but got EOF", value, idx));
    }
    if (value.charAt(idx) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx++;
    idx = ParseUtils.skipSpaces(value, idx);

    if (idx == length) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting CQL identifier or '}', got EOF",
              value, idx));
    }

    String id = null;
    while (idx < length) {
      if (value.charAt(idx) == '}') {
        idx = ParseUtils.skipSpaces(value, idx + 1);
        if (idx == length) {
          return udtValue;
        }
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at character %d expecting EOF or blank, but got \"%s\"",
                value, idx, value.substring(idx)));
      }
      int n = ParseUtils.skipCqlId(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': cannot parse a CQL identifier at character %d",
                value, idx));
      }
      id = value.substring(idx, n);
      idx = n;

      if (!type.columnMap().containsKey(id)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': unknown field name at character %d: \"%s\"",
                value, idx, id));
      }

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at field %s (character %d) expecting ':', but got EOF",
                value, id, idx));
      }
      if (value.charAt(idx) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at field %s (character %d) expecting ':', but got '%c'",
                value, id, idx, value.charAt(idx)));
      }
      idx++;
      idx = ParseUtils.skipSpaces(value, idx);

      n = ParseUtils.skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': invalid CQL value at field %s (character %d)",
                value, id, idx));
      }

      String fieldValue = value.substring(idx, n);
      // This works because ids occur at most once in UDTs
      Column.ColumnType fieldType = type.fieldType(id);
      try {
        udtValue = udtValue.set(id, toCqlValue(fieldType, fieldValue), fieldType.codec());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': invalid CQL value at field %s (character %d): %s",
                value, id, idx, e.getMessage()),
            e);
      }
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (idx == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at field %s (character %d) expecting ',' or '}', but got EOF",
                value, id, idx));
      }
      if (value.charAt(idx) == '}') {
        continue;
      }
      if (value.charAt(idx) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at field %s (character %d) expecting ',' but got '%c'",
                value, id, idx, value.charAt(idx)));
      }
      ++idx; // skip ','

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid UDT value '%s': at field %s (character %d): expecting CQL identifier or '}', got EOF",
            value, id, idx));
  }

  public static Column.Kind getColumnKind(ColumnDefinition def, PrimaryKey primaryKey) {
    // Note: we "rely" on checking the primary key before getIsStatic here. Namely, this allow
    // the caller to check, when this method return a primary key kind, whether static is also set.
    if (primaryKey.getPartitionKey().contains(def.getName())) {
      return Kind.PartitionKey;
    }
    if (primaryKey.getClusteringKey().contains(def.getName())) {
      return Kind.Clustering;
    }
    return def.getIsStatic() ? Kind.Static : Kind.Regular;
  }

  public static Order getColumnOrder(ColumnDefinition def, TableOptions tableOptions)
      throws Exception {
    if (tableOptions.getClusteringExpression() == null
        || tableOptions.getClusteringExpression().size() == 0) {
      return Order.ASC;
    }

    for (ClusteringExpression expression : tableOptions.getClusteringExpression()) {
      if (expression.getOrder() == null || expression.getColumn() == null) {
        throw new Exception("both order and column are required for clustering expression");
      }
      if (def.getName().equals(expression.getColumn())) {
        try {
          return Order.valueOf(expression.getOrder().toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new Exception("order must be either 'asc' or 'desc'");
        }
      }
    }
    return null;
  }

  /**
   * Returns a formatted json response based on provided query parameters.
   *
   * @param response the object to be formatted and returned
   * @return A formatted json string according to user input
   * @throws JsonProcessingException
   */
  public static String writeResponse(Object response) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  public static String maybeQuote(String text) {
    if (UNQUOTED_IDENTIFIER.matcher(text).matches() && !ReservedKeywords.isReserved(text))
      return text;
    return '"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
  }
}
