package io.stargate.db.query;

import static java.lang.String.format;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public class TypedValue {

  /** Placeholder value that can used to represent an UNSET bound value. */
  public static final Object UNSET =
      new Object() {
        @Override
        public String toString() {
          return "<unset>";
        }
      };

  private final ColumnType type;
  private final @Nullable Object javaValue;
  private final @Nullable ByteBuffer bytesValue;

  private TypedValue(ColumnType type, @Nullable Object javaValue, @Nullable ByteBuffer bytesValue) {
    Objects.requireNonNull(type);
    Preconditions.checkArgument(
        bytesValue != null || javaValue == null,
        "The bytes value should have been computed from the java value if the latter is not null");
    this.type = type;
    this.javaValue = javaValue;
    this.bytesValue = bytesValue;
  }

  public ColumnType type() {
    return type;
  }

  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  public boolean isUnset() {
    return javaValue == UNSET;
  }

  public @Nullable Object javaValue() {
    return javaValue;
  }

  public @Nullable ByteBuffer bytes() {
    return bytesValue;
  }

  public static List<Object> javaValues(List<TypedValue> values) {
    List<Object> javaValues = new ArrayList<>(values.size());
    for (TypedValue value : values) {
      javaValues.add(value.javaValue());
    }
    return javaValues;
  }

  public static List<TypedValue> forJavaValues(
      Codec codec, List<BindMarker> markers, List<Object> values) {
    return makeBoundValues(codec, markers, values, TypedValue::forJavaValue);
  }

  public static List<TypedValue> forBytesValues(
      Codec codec, List<BindMarker> markers, List<ByteBuffer> values) {
    return makeBoundValues(codec, markers, values, TypedValue::forBytesValue);
  }

  private static <T> List<TypedValue> makeBoundValues(
      Codec codec, List<BindMarker> markers, List<T> values, BoundValueMaker<T> fct) {
    if (markers.size() != values.size()) {
      throw invalid(
          "Unexpected number of values provided: expect %d values but %d provided",
          markers.size(), values.size());
    }

    List<TypedValue> boundValues = new ArrayList<>(values.size());
    for (int i = 0; i < markers.size(); i++) {
      boundValues.add(fct.make(codec, markers.get(i), values.get(i)));
    }
    return boundValues;
  }

  public static TypedValue forJavaValue(Codec codec, BindMarker marker, @Nullable Object value) {
    return forJavaValue(codec, marker.receiver(), marker.type(), value);
  }

  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  public static TypedValue forJavaValue(
      Codec codec, String name, ColumnType type, @Nullable Object value) {
    if (value != null && value != UNSET) {
      value = validateValue(name, type, value);
    }
    return new TypedValue(type, value, codec.encode(type, value));
  }

  public static TypedValue forBytesValue(
      Codec codec, BindMarker marker, @Nullable ByteBuffer value) {
    return forBytesValue(codec, marker.type(), value);
  }

  public static TypedValue forBytesValue(Codec codec, ColumnType type, @Nullable ByteBuffer value) {

    return new TypedValue(type, codec.decode(type, value), value);
  }

  private static Object validateValue(String name, ColumnType type, Object value) {
    try {
      // For collections, we manually apply our ColumnType#validate method to the sub-elements so
      // that the potential coercions that can happen as part of that validation extend inside
      // collections.
      if (type.isList()) {
        if (!(value instanceof List)) {
          throw invalid(
              "For value bound to %s, expected a list but got a %s (%s)",
              name, value.getClass().getSimpleName(), value);
        }
        ColumnType elementType = type.parameters().get(0);
        List<?> list = (List<?>) value;
        List<Object> validated = new ArrayList<>(list.size());
        for (Object e : list) {
          validated.add(elementType.validate(e, name));
        }
        return validated;
      }
      if (type.isSet()) {
        if (!(value instanceof Set)) {
          throw invalid(
              "For value bound to %s, expected a set but got a %s (%s)",
              name, value.getClass().getSimpleName(), value);
        }
        ColumnType elementType = type.parameters().get(0);
        Set<?> set = (Set<?>) value;
        Set<Object> validated = new HashSet<>();
        for (Object e : set) {
          validated.add(elementType.validate(e, name));
        }
        return validated;
      }
      if (type.isMap()) {
        if (!(value instanceof Map)) {
          throw invalid(
              "For value bound to %s, expected a map but got a %s (%s)",
              name, value.getClass().getSimpleName(), value);
        }
        ColumnType keyType = type.parameters().get(0);
        ColumnType valueType = type.parameters().get(1);
        Map<?, ?> map = (Map<?, ?>) value;
        Map<Object, Object> validated = new HashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
          validated.put(
              keyType.validate(e.getKey(), format("key of map %s", name)),
              valueType.validate(
                  e.getValue(), format("value of map %s for key %s", name, e.getKey())));
        }
        return validated;
      }
      return type.validate(value, name);
    } catch (Column.ValidationException e) {
      throw invalid(
          "Wrong value provided for column '%s'. Provided type '%s' is not compatible with "
              + "expected CQL type '%s'.%s",
          e.location(), e.providedType(), e.expectedCqlType(), e.errorDetails());
    }
  }

  private static IllegalArgumentException invalid(String format, Object... args) {
    return new IllegalArgumentException(format(format, args));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TypedValue)) {
      return false;
    }
    TypedValue that = (TypedValue) o;
    return this.type.equals(that.type) && Objects.equals(bytesValue, that.bytesValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, bytesValue);
  }

  @Override
  @SuppressWarnings("unchecked")
  public String toString() {
    return javaValue == null ? "null" : type.codec().format(javaValue);
  }

  @FunctionalInterface
  private interface BoundValueMaker<T> {
    TypedValue make(Codec codec, BindMarker marker, T value);
  }

  public static class Codec {
    private final ByteBuffer unset;
    private final com.datastax.oss.driver.api.core.ProtocolVersion driverProtocolVersion;

    public Codec(ProtocolVersion protocolVersion, Persistence persistence) {
      this(persistence.unsetValue(), protocolVersion.toDriverVersion());
    }

    private Codec(
        ByteBuffer unset, com.datastax.oss.driver.api.core.ProtocolVersion driverProtocolVersion) {
      this.unset = unset;
      this.driverProtocolVersion = driverProtocolVersion;
    }

    public static Codec testCodec() {
      return new Codec(
          ByteBuffer.allocate(0), com.datastax.oss.driver.api.core.ProtocolVersion.DEFAULT);
    }

    @SuppressWarnings("unchecked")
    public ByteBuffer encode(@Nonnull ColumnType type, Object value) {
      if (value == null) {
        return null;
      } else if (value.equals(TypedValue.UNSET) || value.equals(unset)) {
        return unset;
      } else {
        return type.codec().encode(value, driverProtocolVersion);
      }
    }

    public Object decode(@Nonnull ColumnType type, ByteBuffer value) {
      if (value == null) {
        return null;
      } else if (value.equals(unset)) {
        return TypedValue.UNSET;
      } else {
        return type.codec().decode(value, driverProtocolVersion);
      }
    }
  }
}
