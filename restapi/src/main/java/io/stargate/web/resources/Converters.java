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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.codec.CqlDurationCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.ImmutableValue;
import io.stargate.db.datastore.query.ImmutableWhereCondition;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.WhereCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.ReservedKeywords;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.models.ClusteringExpression;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableOptions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Converters {
  private static final Logger logger = LoggerFactory.getLogger(Converters.class);

  private static final ObjectMapper mapper = new ObjectMapper();

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

  public static WhereCondition<?> idToWhere(String val, String column, Table tableData) {
    Column.ColumnType type = tableData.column(column).type();
    Object value = val;

    if (type != null) {
      value = typeForValue(type, val);
    }

    return ImmutableWhereCondition.builder()
        .value(value)
        .predicate(WhereCondition.Predicate.Eq)
        .column(column.toLowerCase())
        .build();
  }

  public static Value<?> colToValue(String name, String value, Table tableData) {
    Column.ColumnType type = tableData.column(name).type();
    Object valueObj = value;

    if (type != null) {
      valueObj = typeForStringValue(type, value);
    }

    return ImmutableValue.builder().column(name).value(valueObj).build();
  }

  public static Object typeForStringValue(Column.ColumnType type, String value) {
    switch (type.rawType()) {
      case Uuid:
        return UUID.fromString(value);
      case Int:
        return Integer.parseInt(value);
      case Map:
        value = removeSuffix(removePrefix(value, "{"), "}");
        String[] pairs = value.split(",");

        Map<Object, Object> map = new LinkedHashMap<>();
        for (String pair : pairs) {
          String[] keyValue = pair.split(":");
          String key = removePrefix(keyValue[0].trim(), "'");
          key = removeSuffix(key.trim(), "'");
          String mapValue = removePrefix(keyValue[1].trim(), "'");
          mapValue = removeSuffix(mapValue.trim(), "'");

          map.put(
              typeForStringValue(type.parameters().get(0), key),
              typeForStringValue(type.parameters().get(1), mapValue));
        }
        return map;
      case Set:
        value = removeSuffix(removePrefix(value, "{"), "}");
        String[] vals = value.split(",");

        Set<Object> set = new LinkedHashSet<>();
        for (String s : vals) {
          s = removePrefix(s.trim(), "'");
          s = removeSuffix(s.trim(), "'");
          set.add(typeForStringValue(type.parameters().get(0), s));
        }

        return set;
      case Blob:
        return ByteBuffer.wrap(value.getBytes());
      case Date:
        return LocalDate.parse(value);
      case Inet:
        try {
          return InetAddress.getByName(value);
        } catch (UnknownHostException e) {
          // Swallow the error and let the database handle it
          String msg = String.format("Failed to convert value '%s' to InetAddress", value);
          logger.warn(msg);
          throw new IllegalArgumentException(msg);
        }
      case List:
        ObjectMapper mapper = new ObjectMapper();
        try {
          List foo = mapper.readValue(value, List.class);
        } catch (JsonProcessingException e) {
          logger.error("Failed", e);
        }
        value = removeSuffix(removePrefix(value, "["), "]");

        List<Object> list = new ArrayList<>();
        for (String s : value.split(",")) {
          s = removePrefix(s.trim(), "'");
          s = removeSuffix(s.trim(), "'");
          list.add(typeForStringValue(type.parameters().get(0), s));
        }

        return list;
      case Text:
        return value;
      case Time:
        return LocalTime.parse(value);
      case Ascii:
        return value;
      case Float:
        return Float.parseFloat(value);
      case Tuple:
        value = removeSuffix(removePrefix(value, "("), ")");
        ParameterizedType.TupleType tupleType = (ParameterizedType.TupleType) type;
        TupleValue tuple = tupleType.create();

        int count = 0;
        String[] tupleVals = value.split(",");
        //        for (int i = 0; i < type.parameters().size(); i++) {
        //          Column.ColumnType p = type.parameters().get(i);
        //          tuple.set(count++, typeForValue(p, buffer.toString()), p.codec());
        //        }

        return tuple;
      case Bigint:
        return Long.parseLong(value);
      case Double:
        return Double.parseDouble(value);
      case Varint:
        new BigInteger(value);
      case Boolean:
        return Boolean.valueOf(value);
      case Counter:
        return Long.parseLong(value);
      case Decimal:
        return new BigDecimal(value);
      case Tinyint:
        return Byte.valueOf(value);
      case Varchar:
        return value;
      case Duration:
        TypeCodec<CqlDuration> duration = new CqlDurationCodec();
        return duration.parse(value);
      case Smallint:
        return Short.valueOf(value);
      case Timeuuid:
        return UUID.fromString(value);
      case Timestamp:
        return Instant.parse(value);
      case UDT:
        UserDefinedType udt = (UserDefinedType) type;
        udt.checkKeyspaceSet();
        ByteBuffer tupleVal = (ByteBuffer.wrap(value.getBytes())).duplicate();
        UdtValue udtValue = udt.create();

        int udtCount = 0;
        for (Column column : udt.columns()) {
          int size = tupleVal.getInt();
          if (size < 0) {
            udtCount++;
          } else {
            ByteBuffer buffer = tupleVal.slice();
            buffer.limit(size);
            tupleVal.position(tupleVal.position() + size);
            udtValue.set(
                udtCount++,
                typeForStringValue(column.type(), buffer.toString()),
                column.type().codec());
          }
        }

        return udtValue;
    }

    return value;
  }

  public static Value<?> colToValue(Map.Entry<String, String> entry, Table tableData) {
    String name = entry.getKey();
    Column.ColumnType type = tableData.column(name).type();
    Object value = entry.getValue();

    if (type != null) {
      value = typeForValue(type, entry.getValue());
    }

    return ImmutableValue.builder().column(name).value(value).build();
  }

  public static Object typeForValue(Column.ColumnType type, String value) {
    switch (type.rawType()) {
      case Uuid:
        return UUID.fromString(value);
      case Int:
        return Integer.parseInt(value);
      case Map:
        value = removeSuffix(removePrefix(value, "{"), "}");
        String[] pairs = value.split(",");

        Map<Object, Object> map = new LinkedHashMap<>();
        for (String pair : pairs) {
          String[] keyValue = pair.split(":");
          String key = removePrefix(keyValue[0].trim(), "'");
          key = removeSuffix(key.trim(), "'");
          String mapValue = removePrefix(keyValue[1].trim(), "'");
          mapValue = removeSuffix(mapValue.trim(), "'");

          map.put(
              typeForValue(type.parameters().get(0), key),
              typeForValue(type.parameters().get(1), mapValue));
        }
        return map;
      case Set:
        value = removeSuffix(removePrefix(value, "{"), "}");
        String[] vals = value.split(",");

        Set<Object> set = new LinkedHashSet<>();
        for (String s : vals) {
          s = removePrefix(s.trim(), "'");
          s = removeSuffix(s.trim(), "'");
          set.add(typeForValue(type.parameters().get(0), s));
        }

        return set;
      case Blob:
        return ByteBuffer.wrap(value.getBytes());
      case Date:
        return LocalDate.parse(value);
      case Inet:
        try {
          return InetAddress.getByName(value);
        } catch (UnknownHostException e) {
          // Swallow the error and let the database handle it
          String msg = String.format("Failed to convert value '%s' to InetAddress", value);
          logger.warn(msg);
          throw new IllegalArgumentException(msg);
        }
      case List:
        value = removeSuffix(removePrefix(value, "["), "]");

        List<Object> list = new ArrayList<>();
        for (String s : value.split(",")) {
          s = removePrefix(s.trim(), "'");
          s = removeSuffix(s.trim(), "'");
          list.add(typeForValue(type.parameters().get(0), s));
        }

        return list;
      case Text:
        return value;
      case Time:
        return LocalTime.parse(value);
      case Ascii:
        return value;
      case Float:
        return Float.parseFloat(value);
      case Tuple:
        value = removeSuffix(removePrefix(value, "("), ")");
        ParameterizedType.TupleType tupleType = (ParameterizedType.TupleType) type;
        TupleValue tuple = tupleType.create();

        int count = 0;
        String[] tupleVals = value.split(",");
        //        for (int i = 0; i < type.parameters().size(); i++) {
        //          Column.ColumnType p = type.parameters().get(i);
        //          tuple.set(count++, typeForValue(p, buffer.toString()), p.codec());
        //        }

        return tuple;
      case Bigint:
        return Long.parseLong(value);
      case Double:
        return Double.parseDouble(value);
      case Varint:
        return new BigInteger(value);
      case Boolean:
        return Boolean.valueOf(value);
      case Counter:
        return Long.parseLong(value);
      case Decimal:
        return new BigDecimal(value);
      case Tinyint:
        return Byte.valueOf(value);
      case Varchar:
        return value;
      case Duration:
        TypeCodec<CqlDuration> duration = new CqlDurationCodec();
        return duration.parse(value);
      case Smallint:
        return Short.valueOf(value);
      case Timeuuid:
        return UUID.fromString(value);
      case Timestamp:
        return Instant.parse(value);
      case UDT:
        UserDefinedType udt = (UserDefinedType) type;
        udt.checkKeyspaceSet();
        ByteBuffer tupleVal = (ByteBuffer.wrap(value.getBytes())).duplicate();
        UdtValue udtValue = udt.create();

        int udtCount = 0;
        for (Column column : udt.columns()) {
          int size = tupleVal.getInt();
          if (size < 0) {
            udtCount++;
          } else {
            ByteBuffer buffer = tupleVal.slice();
            buffer.limit(size);
            tupleVal.position(tupleVal.position() + size);
            udtValue.set(
                udtCount++, typeForValue(column.type(), buffer.toString()), column.type().codec());
          }
        }

        return udtValue;
    }

    return value;
  }

  private static String removePrefix(final String str, final String prefix) {
    if (str != null && prefix != null && str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }

  private static String removeSuffix(final String str, final String suffix) {
    if (str != null && suffix != null && str.endsWith(suffix)) {
      return str.substring(0, str.length() - suffix.length());
    }
    return str;
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

  public static String getTableOptions(TableAdd tableAdd) throws Exception {
    TableOptions options = tableAdd.getTableOptions();
    if (options == null) {
      return "";
    }

    String tableOptions = "";
    if (options.getDefaultTimeToLive() != null) {
      tableOptions = getOptionPrefix(tableOptions);

      tableOptions += "default_time_to_live = " + options.getDefaultTimeToLive();
    }

    if (options.getClusteringExpression() != null && options.getClusteringExpression().size() > 0) {
      tableOptions = getOptionPrefix(tableOptions);

      String expression = "";
      for (int i = 0; i < options.getClusteringExpression().size(); i++) {
        ClusteringExpression exp = options.getClusteringExpression().get(i);
        if (exp.getOrder() == null || exp.getColumn() == null) {
          throw new Exception("both order and column are required for clustering expression");
        }

        if (!exp.getOrder().equalsIgnoreCase("asc") && !exp.getOrder().equalsIgnoreCase("desc")) {
          throw new Exception("order must be either 'asc' or 'desc'");
        }

        if (i == options.getClusteringExpression().size() - 1) {
          expression += exp.getColumn() + " " + exp.getOrder();
        }
      }

      tableOptions += String.format("CLUSTERING ORDER BY (%s)", expression);
    }

    return tableOptions;
  }

  private static String getOptionPrefix(String tableOptions) {
    if (tableOptions.equals("")) {
      tableOptions = "WITH ";
    } else {
      tableOptions += " AND ";
    }
    return tableOptions;
  }

  private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
  private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
  private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");

  public static String maybeQuote(String text) {
    if (UNQUOTED_IDENTIFIER.matcher(text).matches() && !ReservedKeywords.isReserved(text))
      return text;
    return '"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
  }
}
