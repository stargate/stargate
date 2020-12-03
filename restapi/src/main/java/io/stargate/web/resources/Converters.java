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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.ParameterizedType.TupleType;
import io.stargate.db.schema.ReservedKeywords;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.models.ClusteringExpression;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.TableOptions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
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

public class Converters {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
  private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
  private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");

  static {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static Map<String, Object> row2Map(final Row row) {
    final List<Column> defs = row.columns();
    final Map<String, Object> map = new HashMap<>(defs.size());
    for (final Column column : defs) {

      map.put(column.name(), transformObjectToJavaObject(row.getObject(column.name())));
    }
    return map;
  }

  private static Object transformObjectToJavaObject(final Object o) {
    if (o instanceof Object[]) {
      return new ArrayList<>(Arrays.asList((Object[]) o));
    } else {
      return o;
    }
  }

  public static BuiltCondition idToWhere(String val, String column, Table tableData) {
    Column.ColumnType type = tableData.column(column).type();
    Object value = val;

    if (type != null) {
      value = parse(type, val);
    }

    return BuiltCondition.of(column.toLowerCase(), Predicate.EQ, value);
  }

  public static ValueModifier colToValue(String name, Object value, Table tableData) {
    Column.ColumnType type = tableData.column(name).type();
    Object valueObj = value;

    if (type != null) {
      valueObj = coerce(type, value);
    }

    return ValueModifier.set(name, valueObj);
  }

  /** Converts an incoming JSON value into a Java type suitable for the given column type. */
  @SuppressWarnings("unchecked")
  public static Object coerce(Column.ColumnType type, Object value) {

    if (value instanceof String) {
      return parse(type, (String) value);
    }

    switch (type.rawType()) {
      case Text:
      case Varchar:
      case Ascii:
      case Uuid:
      case Timeuuid:
      case Blob:
      case Inet:
      case Duration:
        throw new IllegalArgumentException(
            String.format("Invalid %s value: expected a string", type.rawType()));
      case Boolean:
        if (value instanceof Boolean) {
          return value;
        } else {
          throw new IllegalArgumentException(
              "Invalid Boolean value: expected a boolean or a string");
        }
      case Tinyint:
        return coerceInt(
            type,
            value,
            BigInteger::shortValueExact,
            Number::byteValue,
            Byte.MIN_VALUE,
            Byte.MAX_VALUE);
      case Smallint:
        return coerceInt(
            type,
            value,
            BigInteger::shortValueExact,
            Number::shortValue,
            Short.MIN_VALUE,
            Short.MAX_VALUE);
      case Int:
        return coerceInt(
            type,
            value,
            BigInteger::intValueExact,
            Number::intValue,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE);
      case Bigint:
      case Counter:
        return coerceInt(
            type,
            value,
            BigInteger::longValueExact,
            Number::longValue,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      case Varint:
        if (value instanceof BigInteger) {
          return value;
        } else if (value instanceof Integer || value instanceof Long) {
          return BigInteger.valueOf(((Number) value).longValue());
        } else {
          throw new IllegalArgumentException(
              "Invalid Varint value: expected an integer or a string");
        }
      case Float:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        } else {
          throw new IllegalArgumentException("Invalid Float value: expected a number or a string");
        }
      case Double:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        } else {
          throw new IllegalArgumentException("Invalid Double value: expected a number or a string");
        }
      case Decimal:
        if (value instanceof BigDecimal) {
          return value;
        } else if (value instanceof Number) {
          return BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
          throw new IllegalArgumentException(
              "Invalid Decimal value: expected a number or a string");
        }
      case Date:
        if (value instanceof Integer || value instanceof Long) {
          return EPOCH.plusDays(cqlDateToDaysSinceEpoch(((Number) value).longValue()));
        } else {
          throw new IllegalArgumentException("Invalid Date value: expected an integer or a string");
        }
      case Time:
        if (value instanceof Integer || value instanceof Long) {
          return LocalTime.ofNanoOfDay(((Number) value).longValue());
        } else {
          throw new IllegalArgumentException("Invalid Time value: expected an integer or a string");
        }
      case Timestamp:
        if (value instanceof Integer || value instanceof Long) {
          return Instant.ofEpochMilli(((Number) value).longValue());
        } else {
          throw new IllegalArgumentException(
              "Invalid Timestamp value: expected an integer or a string");
        }
      case List:
        if (value instanceof List) {
          return coerceCollection(
              type, (List<Object>) value, ArrayList::new, Collections.emptyList());
        } else {
          throw new IllegalArgumentException(
              "Invalid List value: expected a JSON array or a string");
        }
      case Set:
        if (value instanceof List) {
          return coerceCollection(
              type, (List<Object>) value, LinkedHashSet::new, Collections.emptySet());
        } else {
          throw new IllegalArgumentException(
              "Invalid Set value: expected a JSON array or a string");
        }
      case Map:
        if (value instanceof List) {
          return coerceMap(type, (List<Object>) value);
        } else {
          throw new IllegalArgumentException(
              "Invalid Map value: expected a JSON array of key/value objects, or a string");
        }
      case Tuple:
        if (value instanceof List) {
          return coerceTuple((TupleType) type, (List<Object>) value);
        } else {
          throw new IllegalArgumentException(
              "Invalid Tuple value: expected a JSON array or a string");
        }
      case UDT:
        if (value instanceof Map) {
          return coerceUdt((UserDefinedType) type, (Map<String, Object>) value);
        } else {
          throw new IllegalArgumentException(
              "Invalid UDT value: expected a JSON object or a string");
        }
      default:
        throw new AssertionError("Unsupported data type: " + type.rawType());
    }
  }

  private static <I> I coerceInt(
      Column.ColumnType type,
      Object value,
      Function<BigInteger, I> fromBigIntegerExact,
      Function<Number, I> fromNumber,
      long min,
      long max) {
    if (value instanceof BigInteger) {
      try {
        return fromBigIntegerExact.apply((BigInteger) value);
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            String.format("Invalid %s value %s: out of range", type.rawType(), value));
      }
    }
    if (value instanceof Integer || value instanceof Long) {
      Number number = (Number) value;
      long longValue = number.longValue();
      if (longValue < min || longValue > max) {
        throw new IllegalArgumentException(
            String.format("Invalid %s value %s: out of range", type.rawType(), value));
      }
      return fromNumber.apply(number);
    }
    throw new IllegalArgumentException("Invalid %s value: expected an integer or a string");
  }

  private static <C extends Collection<Object>> C coerceCollection(
      Column.ColumnType type, List<Object> values, Supplier<C> newInstance, C empty) {

    if (values.isEmpty()) {
      return empty;
    }
    Column.ColumnType elementType = type.parameters().get(0);
    C result = newInstance.get();
    int index = 0;
    for (Object value : values) {
      try {
        result.add(coerce(elementType, value));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s element at index %d (%s)", type.rawType(), index, e.getMessage()));
      }
      index += 1;
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  private static Map<Object, Object> coerceMap(Column.ColumnType type, List<Object> entries) {
    if (entries.isEmpty()) {
      return Collections.emptyMap();
    }
    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);
    LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
    int index = 0;
    for (Object entry : entries) {
      if (!(entry instanceof Map)
          || !((Map) entry).containsKey("key")
          || !((Map) entry).containsKey("value")) {
        throw new IllegalArgumentException(
            "Invalid Map value: inner elements must be objects with two fields 'key' and 'value'");
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> entryMap = (Map<String, Object>) entry;
      Object key, value;
      try {
        key = coerce(keyType, entryMap.get("key"));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid Map key at index %d (%s)", index, e.getMessage()));
      }
      try {
        value = entryMap.get("value");
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid Map value at index %d (%s)", index, e.getMessage()));
      }
      result.put(key, coerce(valueType, value));
      index += 1;
    }
    return result;
  }

  private static TupleValue coerceTuple(TupleType type, List<Object> values) {
    List<Object> fields = Lists.newArrayListWithCapacity(values.size());
    int index = 0;
    for (Object value : values) {
      try {
        fields.add(coerce(type.parameters().get(index), value));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid Tuple field at index %d (%s)", index, e.getMessage()));
      }
      index += 1;
    }
    return type.create(fields.toArray());
  }

  @SuppressWarnings("unchecked")
  private static UdtValue coerceUdt(UserDefinedType type, Map<String, Object> values) {
    UdtValue udtValue = type.create();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String fieldId = entry.getKey();
      Object fieldValue = entry.getValue();
      if (!type.columnMap().containsKey(fieldId)) {
        throw new IllegalArgumentException(
            String.format("Invalid UDT value: unknown field name \"%s\"", fieldId));
      }
      Column.ColumnType fieldType = type.fieldType(fieldId);
      try {
        udtValue = udtValue.set(fieldId, coerce(fieldType, fieldValue), fieldType.codec());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid UDT field %s (%s)", fieldId, e.getMessage()), e);
      }
    }
    return udtValue;
  }

  private static int cqlDateToDaysSinceEpoch(long raw) {
    if (raw < 0 || raw > MAX_CQL_LONG_VALUE)
      throw new IllegalArgumentException(
          String.format(
              "Invalid Date value: numeric literals must be between 0 and %d (got %d)",
              MAX_CQL_LONG_VALUE, raw));
    return (int) (raw - EPOCH_AS_CQL_LONG);
  }

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
  private static final long MAX_CQL_LONG_VALUE = ((1L << 32) - 1);
  private static final long EPOCH_AS_CQL_LONG = (1L << 31);

  /**
   * Converts an incoming JSON string value into a Java type suitable for the given column type.
   *
   * <p>This method exists separately from {@link #coerce(Column.ColumnType, Object)} for the v1
   * REST API, which only allows strings.
   */
  public static Object parse(Column.ColumnType type, String value) {
    value = value.trim();
    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1).replaceAll("''", "'");
    }

    Column.Type rawType = type.rawType();
    // Handle complex types separately because they already handle parsing errors:
    if (rawType == Column.Type.List) {
      return parseCollection(type, value, ArrayList::new, Collections.emptyList(), '[', ']');
    } else if (rawType == Column.Type.Set) {
      return parseCollection(type, value, LinkedHashSet::new, Collections.emptySet(), '{', '}');
    } else if (rawType == Column.Type.Map) {
      return parseMap(type, value);
    } else if (rawType == Column.Type.Tuple) {
      return parseTuple((TupleType) type, value);
    } else if (rawType == Column.Type.UDT) {
      return parseUdt((UserDefinedType) type, value);
    } else {
      try {
        switch (rawType) {
          case Text:
          case Varchar:
          case Ascii:
            return value;
          case Boolean:
            return Boolean.valueOf(value);
          case Tinyint:
            return Byte.valueOf(value);
          case Smallint:
            return Short.valueOf(value);
          case Int:
            return Integer.valueOf(value);
          case Bigint:
          case Counter:
            return Long.valueOf(value);
          case Float:
            return Float.parseFloat(value);
          case Double:
            return Double.parseDouble(value);
          case Varint:
            return new BigInteger(value);
          case Decimal:
            return new BigDecimal(value);
          case Uuid:
          case Timeuuid:
            return UUID.fromString(value);
          case Blob:
            return ByteBuffer.wrap(Base64.getDecoder().decode(value));
          case Inet:
            return InetAddress.getByName(value);
          case Date:
            return LocalDate.parse(value);
          case Time:
            return LocalTime.parse(value);
          case Timestamp:
            return Instant.parse(value);
          case Duration:
            return CqlDuration.from(value);
          default:
            throw new AssertionError("Unsupported data type: " + rawType);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid %s value: %s", rawType, e.getMessage()));
      }
    }
  }

  private static <C extends Collection<Object>> C parseCollection(
      Column.ColumnType type,
      String value,
      Supplier<C> newInstance,
      C empty,
      char openingBrace,
      char closingBrace) {
    int idx = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(idx++) != openingBrace) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value: at character %d expecting '%s' but got '%c'",
              type.rawType(), idx, openingBrace, value.charAt(idx)));
    }
    idx = ParseUtils.skipSpaces(value, idx);
    if (value.charAt(idx) == closingBrace) {
      return empty;
    }
    C collection = newInstance.get();
    Column.ColumnType elementType = type.parameters().get(0);
    while (idx < value.length()) {
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value: invalid CQL value at character %d", type.rawType(), idx),
            e);
      }

      collection.add(parse(elementType, value.substring(idx, n)));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx) == closingBrace) {
        return collection;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value: at character %d expecting ',' but got '%c'",
                type.rawType(), idx, value.charAt(idx)));
      }

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Invalid %s value: missing closing '%s'", type.rawType(), closingBrace));
  }

  private static Map<Object, Object> parseMap(Column.ColumnType type, String value) {
    int idx = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(idx++) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid map value: at character %d expecting '{' but got '%c'",
              idx, value.charAt(idx)));
    }

    idx = ParseUtils.skipSpaces(value, idx);

    if (value.charAt(idx) == '}') {
      return Collections.emptyMap();
    }

    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);

    Map<Object, Object> map = new LinkedHashMap<>();
    while (idx < value.length()) {
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid map value: invalid CQL value at character %d", idx), e);
      }

      Object k = parse(keyType, value.substring(idx, n));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx++) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value: at character %d expecting ':' but got '%c'",
                idx, value.charAt(idx)));
      }
      idx = ParseUtils.skipSpaces(value, idx);

      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid map value: invalid CQL value at character %d", idx), e);
      }

      Object v = parse(valueType, value.substring(idx, n));
      idx = n;

      map.put(k, v);

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx) == '}') {
        return map;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value: at character %d expecting ',' but got '%c'",
                idx, value.charAt(idx)));
      }

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException("Invalid map value: missing closing '}'");
  }

  private static TupleValue parseTuple(TupleType type, String value) {
    List<Object> fields = new ArrayList<>();
    int length = value.length();

    int position = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(position) != '(') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid tuple value: at character %d expecting '(' but got '%c'",
              position, value.charAt(position)));
    }

    position++;
    position = ParseUtils.skipSpaces(value, position);

    int fieldIndex = 0;
    while (position < length) {
      if (value.charAt(position) == ')') {
        position = ParseUtils.skipSpaces(value, position + 1);
        if (position == length) {
          return type.create(fields.toArray());
        }
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value: at character %d expecting EOF or blank, but got \"%s\"",
                position, value.substring(position)));
      }
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, position);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value: invalid CQL value at field %d (character %d)",
                fieldIndex, position),
            e);
      }

      String fieldValue = value.substring(position, n);
      try {
        fields.add(parse(type.parameters().get(fieldIndex), fieldValue));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value: invalid CQL value at field %d (character %d): %s",
                fieldIndex, position, e.getMessage()),
            e);
      }

      position = n;

      position = ParseUtils.skipSpaces(value, position);
      if (position == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value: at field %d (character %d) expecting ',' or ')', but got EOF",
                fieldIndex, position));
      }
      if (value.charAt(position) == ')') {
        continue;
      }
      if (value.charAt(position) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value, at field %d (character %d) expecting ',' but got '%c'",
                fieldIndex, position, value.charAt(position)));
      }
      ++position; // skip ','

      position = ParseUtils.skipSpaces(value, position);
      fieldIndex += 1;
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid tuple value: at field %d (character %d) expecting CQL value or ')', got EOF",
            fieldIndex, position));
  }

  @SuppressWarnings("unchecked")
  private static UdtValue parseUdt(UserDefinedType type, String value) {
    UdtValue udtValue = type.create();
    int length = value.length();

    int position = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(position) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value: at character %d: expecting '{' but got '%c'",
              position, value.charAt(position)));
    }

    position++;
    position = ParseUtils.skipSpaces(value, position);

    if (position == length) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value: at character %d: expecting CQL identifier or '}', got EOF",
              position));
    }

    String id = null;
    while (position < length) {
      if (value.charAt(position) == '}') {
        position = ParseUtils.skipSpaces(value, position + 1);
        if (position == length) {
          return udtValue;
        }
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: at character %d expecting EOF or blank, but got \"%s\"",
                position, value.substring(position)));
      }
      int n;
      try {
        n = ParseUtils.skipCQLId(value, position);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: cannot parse a CQL identifier at character %d", position),
            e);
      }
      id = value.substring(position, n);
      position = n;

      if (!type.columnMap().containsKey(id)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: unknown field name at character %d: \"%s\"", position, id));
      }

      position = ParseUtils.skipSpaces(value, position);
      if (position == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: at field %s (character %d) expecting ':', but got EOF",
                id, position));
      }
      if (value.charAt(position) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: at field %s (character %d) expecting ':', but got '%c'",
                id, position, value.charAt(position)));
      }
      position++;
      position = ParseUtils.skipSpaces(value, position);

      try {
        n = ParseUtils.skipCQLValue(value, position);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: invalid CQL value at field %s (character %d)", id, position),
            e);
      }

      String fieldValue = value.substring(position, n);
      // This works because ids occur at most once in UDTs
      Column.ColumnType fieldType = type.fieldType(id);
      try {
        udtValue = udtValue.set(id, parse(fieldType, fieldValue), fieldType.codec());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: invalid CQL value at field %s (character %d): %s",
                id, position, e.getMessage()),
            e);
      }
      position = n;

      position = ParseUtils.skipSpaces(value, position);
      if (position == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: at field %s (character %d) expecting ',' or '}', but got EOF",
                id, position));
      }
      if (value.charAt(position) == '}') {
        continue;
      }
      if (value.charAt(position) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value: at field %s (character %d) expecting ',' but got '%c'",
                id, position, value.charAt(position)));
      }
      ++position; // skip ','

      position = ParseUtils.skipSpaces(value, position);
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid UDT value: at field %s (character %d): expecting CQL identifier or '}', got EOF",
            id, position));
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
    return mapper.writeValueAsString(response);
  }

  public static String maybeQuote(String text) {
    if (UNQUOTED_IDENTIFIER.matcher(text).matches() && !ReservedKeywords.isReserved(text))
      return text;
    return '"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
  }
}
