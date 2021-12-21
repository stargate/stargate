/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.datastore.common.util;

import static java.util.stream.Collectors.toList;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.immutables.value.Value;

public class ColumnUtils {
  public static final Pattern WHITESPACE_PATTERN = Pattern.compile("(?U)\\s");
  private static final ZoneId UTC = ZoneId.of("UTC");

  @Value.Immutable(prehash = true)
  abstract static class Codecs {
    abstract AbstractType internalType();

    @Value.Default
    Function<Object, Object> externalConversion() {
      return Function.identity();
    }

    @Value.Default
    Function<Object, Object> internalConversion() {
      return Function.identity();
    }
  }

  public static final EnumMap<Column.Type, ColumnUtils.Codecs> CODECS =
      new EnumMap<>(Column.Type.class);

  static {
    CODECS.put(
        Column.Type.Ascii, ImmutableCodecs.builder().internalType(AsciiType.instance).build());
    CODECS.put(
        Column.Type.Bigint, ImmutableCodecs.builder().internalType(LongType.instance).build());
    CODECS.put(
        Column.Type.Blob, ImmutableCodecs.builder().internalType(BytesType.instance).build());
    CODECS.put(
        Column.Type.Boolean, ImmutableCodecs.builder().internalType(BooleanType.instance).build());
    CODECS.put(
        Column.Type.Counter,
        ImmutableCodecs.builder().internalType(CounterColumnType.instance).build());
    CODECS.put(
        Column.Type.Date,
        ImmutableCodecs.builder()
            .internalType(SimpleDateType.instance)
            .externalConversion(
                internal ->
                    LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(
                                SimpleDateSerializer.dayToTimeInMillis((int) internal)),
                            UTC)
                        .toLocalDate())
            .internalConversion(
                external ->
                    SimpleDateSerializer.timeInMillisToDay(
                        ((LocalDate) external)
                            .atStartOfDay()
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli()))
            .build());
    CODECS.put(
        Column.Type.Decimal, ImmutableCodecs.builder().internalType(DecimalType.instance).build());
    CODECS.put(
        Column.Type.Double, ImmutableCodecs.builder().internalType(DoubleType.instance).build());
    CODECS.put(
        Column.Type.Duration,
        ImmutableCodecs.builder()
            .internalType(DurationType.instance)
            .externalConversion(
                internal -> {
                  org.apache.cassandra.cql3.Duration d =
                      (org.apache.cassandra.cql3.Duration) internal;
                  return CqlDuration.newInstance(d.getMonths(), d.getDays(), d.getNanoseconds());
                })
            .internalConversion(
                external -> {
                  CqlDuration d = (CqlDuration) external;
                  return org.apache.cassandra.cql3.Duration.newInstance(
                      d.getMonths(), d.getDays(), d.getNanoseconds());
                })
            .build());
    CODECS.put(
        Column.Type.Float, ImmutableCodecs.builder().internalType(FloatType.instance).build());
    CODECS.put(
        Column.Type.Inet, ImmutableCodecs.builder().internalType(InetAddressType.instance).build());
    CODECS.put(Column.Type.Int, ImmutableCodecs.builder().internalType(Int32Type.instance).build());
    CODECS.put(
        Column.Type.Smallint, ImmutableCodecs.builder().internalType(ShortType.instance).build());
    CODECS.put(Column.Type.Text, ImmutableCodecs.builder().internalType(UTF8Type.instance).build());
    CODECS.put(
        Column.Type.Time,
        ImmutableCodecs.builder()
            .internalType(TimeType.instance)
            .externalConversion(internal -> LocalTime.ofNanoOfDay(((long) internal)))
            .internalConversion(external -> ((LocalTime) external).toNanoOfDay())
            .build());
    CODECS.put(
        Column.Type.Timestamp,
        ImmutableCodecs.builder()
            .internalType(TimestampType.instance)
            .externalConversion(internal -> ((Date) internal).toInstant())
            .internalConversion(external -> new Date(((Instant) external).toEpochMilli()))
            .build());
    CODECS.put(
        Column.Type.Timeuuid,
        ImmutableCodecs.builder().internalType(TimeUUIDType.instance).build());
    CODECS.put(
        Column.Type.Tinyint, ImmutableCodecs.builder().internalType(ByteType.instance).build());
    CODECS.put(Column.Type.Uuid, ImmutableCodecs.builder().internalType(UUIDType.instance).build());
    CODECS.put(
        Column.Type.Varchar, ImmutableCodecs.builder().internalType(UTF8Type.instance).build());
    CODECS.put(
        Column.Type.Varint, ImmutableCodecs.builder().internalType(IntegerType.instance).build());
  }

  public static AbstractType toInternalType(Column.ColumnType type) {
    switch (type.rawType()) {
      case List:
        return org.apache.cassandra.db.marshal.ListType.getInstance(
            toInternalType(type.parameters().get(0)), !type.isFrozen());
      case Set:
        return org.apache.cassandra.db.marshal.SetType.getInstance(
            toInternalType(type.parameters().get(0)), !type.isFrozen());
      case Map:
        return org.apache.cassandra.db.marshal.MapType.getInstance(
            toInternalType(type.parameters().get(0)),
            toInternalType(type.parameters().get(1)),
            !type.isFrozen());
      case Tuple:
        return new org.apache.cassandra.db.marshal.TupleType(
            type.parameters().stream()
                .map(p -> toInternalType(p))
                .collect(toList())); // TODO: Always frozen?
      case UDT:
        UserDefinedType udt = (UserDefinedType) type;
        udt.checkKeyspaceSet();
        return new UserType(
            udt.keyspace(),
            ByteBuffer.wrap(udt.name().getBytes(StandardCharsets.UTF_8)),
            udt.columns().stream()
                .map(c -> FieldIdentifier.forInternalString(c.name()))
                .collect(toList()),
            udt.columns().stream().map(c -> toInternalType(c.type())).collect(toList()),
            !type.isFrozen());
      default:
        return CODECS.get(type.rawType()).internalType();
    }
  }

  public static Object toInternalValue(Column.ColumnType type, Object external) {
    switch (type.rawType()) {
      case List:
        return toInternalList(type, external);
      case Set:
        return toInternalSet(type, external);
      case Map:
        return toInternalMap(type, external);
      case Tuple:
        return toInternalTuple(type, external);
      case UDT:
        return toInternalUdt(type, external);
      default:
        return CODECS.get(type.rawType()).internalConversion().apply(external);
    }
  }

  private static Object toInternalList(Column.ColumnType type, Object external) {
    Column.ColumnType columnType = type.parameters().get(0);
    List value = (List) external;
    List<Object> internal = new ArrayList<>();
    for (Object o : value) {
      internal.add(toInternalValue(columnType, o));
    }
    return internal;
  }

  private static Object toInternalSet(Column.ColumnType type, Object external) {
    Column.ColumnType columnType = type.parameters().get(0);
    Set value = (Set) external;
    Set<Object> internal = new LinkedHashSet<>();
    for (Object o : value) {
      internal.add(toInternalValue(columnType, o));
    }
    return internal;
  }

  private static Object toInternalMap(Column.ColumnType type, Object external) {
    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);
    Map<Object, Object> value = (Map) external;
    Map<Object, Object> internal = new LinkedHashMap<>();
    for (Map.Entry<Object, Object> e : value.entrySet()) {
      internal.put(toInternalValue(keyType, e.getKey()), toInternalValue(valueType, e.getValue()));
    }
    return internal;
  }

  private static Object toInternalTuple(Column.ColumnType type, Object external) {
    List<ByteBuffer> byteBuffers = new ArrayList<>();
    TupleValue value = (TupleValue) external;
    int count = 0;
    for (Column.ColumnType p : type.parameters()) {
      Object field = value.get(count++, p.codec());
      if (field == null) {
        byteBuffers.add(null);
      } else {
        Object internalValue = toInternalValue(p, field);
        ByteBuffer bytes = toInternalType(p).decompose(internalValue).duplicate();
        byteBuffers.add(bytes);
      }
    }
    int size = 0;
    for (ByteBuffer buffer : byteBuffers) {
      size += 4;
      if (buffer != null) {
        size += buffer.limit();
      }
    }

    ByteBuffer internal = ByteBuffer.allocate(size);
    for (ByteBuffer buffer : byteBuffers) {
      if (buffer == null) {
        internal.putInt(-1);
      } else {
        internal.putInt(buffer.limit());
        internal.put(buffer);
      }
    }
    internal.rewind();
    return internal;
  }

  private static Object toInternalUdt(Column.ColumnType type, Object external) {
    UserDefinedType udt = (UserDefinedType) type;
    udt.checkKeyspaceSet();
    List<ByteBuffer> byteBuffers = new ArrayList<>(udt.columns().size());
    UdtValue value = (UdtValue) external;

    int count = 0;
    for (Column column : udt.columns()) {
      Object field = value.get(count++, column.type().codec());
      if (field == null) {
        byteBuffers.add(null);
      } else {
        Object internalValue = toInternalValue(column.type(), field);
        ByteBuffer bytes = toInternalType(column.type()).decompose(internalValue).duplicate();
        byteBuffers.add(bytes);
      }
    }

    int size = 0;
    for (ByteBuffer buffer : byteBuffers) {
      size += 4;
      if (buffer != null) {
        size += buffer.limit();
      }
    }

    ByteBuffer internal = ByteBuffer.allocate(size);
    for (ByteBuffer buffer : byteBuffers) {
      if (buffer == null) {
        internal.putInt(-1);
      } else {
        internal.putInt(buffer.limit());
        internal.put(buffer);
      }
    }
    internal.rewind();

    return internal;
  }

  public static Object toExternalValue(Column.ColumnType type, Object internal) {
    switch (type.rawType()) {
      case List:
        return toExternalList(type, internal);
      case Set:
        return toExternalSet(type, internal);
      case Map:
        return toExternalMap(type, internal);
      case Tuple:
        return toExternalTuple(type, internal);
      case UDT:
        return toExternalUdt(type, internal);
      default:
        return CODECS.get(type.rawType()).externalConversion().apply(internal);
    }
  }

  private static Object toExternalList(Column.ColumnType type, Object internal) {
    Column.ColumnType columnType = type.parameters().get(0);
    List value = (List) internal;
    List<Object> external = new ArrayList<>();
    for (Object o : value) {
      external.add(toExternalValue(columnType, o));
    }
    return external;
  }

  private static Object toExternalSet(Column.ColumnType type, Object internal) {
    Column.ColumnType columnType = type.parameters().get(0);
    Set value = (Set) internal;
    Set<Object> external = new LinkedHashSet<>();
    for (Object o : value) {
      external.add(toExternalValue(columnType, o));
    }
    return external;
  }

  private static Object toExternalMap(Column.ColumnType type, Object internal) {
    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);
    Map<Object, Object> value = (Map) internal;
    Map<Object, Object> external = new LinkedHashMap<>();
    for (Map.Entry<Object, Object> e : value.entrySet()) {
      external.put(toExternalValue(keyType, e.getKey()), toExternalValue(valueType, e.getValue()));
    }
    return external;
  }

  private static Object toExternalTuple(Column.ColumnType type, Object internal) {
    ByteBuffer value = ((ByteBuffer) internal).duplicate();
    ParameterizedType.TupleType tupleType = (ParameterizedType.TupleType) type;
    TupleValue tuple = tupleType.create();

    int count = 0;
    for (Column.ColumnType p : type.parameters()) {
      int size = value.getInt();
      if (size < 0) {
        count++;
      } else {
        ByteBuffer buffer = value.slice();
        buffer.limit(size);
        value.position(value.position() + size);
        tuple =
            tuple.set(count++, toExternalValue(p, toInternalType(p).compose(buffer)), p.codec());
      }
    }

    return tuple;
  }

  private static Object toExternalUdt(Column.ColumnType type, Object internal) {
    UserDefinedType udt = (UserDefinedType) type;
    udt.checkKeyspaceSet();
    ByteBuffer value = ((ByteBuffer) internal).duplicate();
    UdtValue udtValue = udt.create();

    int count = 0;
    for (Column column : udt.columns()) {
      int size = value.getInt();
      if (size < 0) {
        count++;
      } else {
        ByteBuffer buffer = value.slice();
        buffer.limit(size);
        value.position(value.position() + size);
        udtValue =
            udtValue.set(
                count++,
                toExternalValue(column.type(), toInternalType(column.type()).compose(buffer)),
                column.type().codec());
      }
    }

    return udtValue;
  }
}
