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
package io.stargate.db.schema;

import static io.stargate.db.schema.Column.Kind.Regular;
import static io.stargate.db.schema.Column.Type.Ascii;
import static io.stargate.db.schema.Column.Type.Counter;
import static io.stargate.db.schema.Column.Type.Timeuuid;
import static io.stargate.db.schema.Column.Type.Varchar;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class Column implements SchemaEntity, Comparable<Column> {
  private static final long serialVersionUID = 2199514488330101566L;

  public static final CodecRegistry CODEC_REGISTRY;

  static {
    DefaultCodecRegistry registry = new DefaultCodecRegistry("persistence-codec-registry");
    CODEC_REGISTRY = registry;
  }

  public enum Order {
    Asc,
    Desc;

    Order reversed() {
      if (this == Asc) {
        return Desc;
      } else {
        return Asc;
      }
    }
  }

  public static final Column STAR = reference("*");
  public static final Column TTL = Column.create("[ttl]", Type.Int);
  public static final Column TIMESTAMP = Column.create("[timestamp]", Type.Bigint);

  public interface ColumnType extends java.io.Serializable {
    AttachmentPoint CUSTOM_ATTACHMENT_POINT =
        new AttachmentPoint() {

          @Override
          public ProtocolVersion getProtocolVersion() {
            return ProtocolVersion.DEFAULT;
          }

          /**
           * make sure to NEVER use CODEC_REGISTRY.codecFor(dataType). Instead, use
           * DataStoreUtil.getTypeFromDriver(dataType).codec() simply because we use internally
           * slightly different ColumnType <-> Codec mappings than the Driver actually does. One
           * example is that CODEC_REGISTRY.codecFor(DATE) will return DateCodec, but on the server
           * (ColumnUtils) we actually want to use LocalDateCodec for a DATE
           */
          @Override
          public CodecRegistry getCodecRegistry() {
            return CODEC_REGISTRY;
          }
        };

    int id();

    Type rawType();

    Class<?> javaType();

    default boolean isParameterized() {
      return false;
    }

    @Value.Default
    default boolean isFrozen() {
      return false;
    }

    default ColumnType frozen(boolean frozen) {
      return this;
    }

    default ColumnType frozen() {
      return frozen(true);
    }

    default ColumnType of(ColumnType... types) {
      throw new UnsupportedOperationException("ColumnType.of() not supported by " + rawType());
    }

    /**
     * Will validate the given value against the underlying column type using <code>instanceof
     * </code>. If the value is a {@link Number} and an <code>instanceof</code> fails, it will also
     * try to check if the value can be narrowed/widened to the given column type using the rules
     * described in {@link NumberCoercion#coerceToColumnType(Class, Object)}.
     *
     * @param value The value to validate.
     * @param location The location of the value (e.g. within a complex type, such as a UDT/Tuple).
     * @return either the {@code value} argument if it is valid for this type without coercion, or,
     *     if coercion is necessary to make it valid, the coerced value.
     * @throws ValidationException In case validation fails.
     */
    default Object validate(Object value, String location) throws ValidationException {
      if (value == null || javaType().isInstance(value)) {
        return value;
      }

      try {
        NumberCoercion.Result result = NumberCoercion.coerceToColumnType(javaType(), value);
        if (!result.columnTypeAcceptsValue()) {
          throw new ValidationException(value.getClass(), this, location);
        }
        return result.getValidatedValue();
      } catch (ArithmeticException e) {
        throw new ValidationException(value.getClass(), this, e.getMessage(), location);
      }
    }

    /** The full CQL definition for this type */
    @Value.Lazy
    String cqlDefinition();

    String name();

    @Value.Lazy
    TypeCodec codec();

    default String toString(Object value) {
      Preconditions.checkNotNull(value, "Parameter value cannot be null");
      try {
        return codec().format(validate(value, "unknown"));
      } catch (Exception e) {
        if (!codec().accepts(value)) {
          throw new IllegalArgumentException(
              String.format(
                  "Codec '%s' cannot process value of type '%s'",
                  codec().getClass().getSimpleName(), value.getClass().getName()),
              e);
        } else {
          throw new IllegalStateException(e);
        }
      }
    }

    default Object fromString(String value) {
      return codec().parse(value);
    }

    default <T> T create(Object... parameters) {
      throw new UnsupportedOperationException(
          "ColumnType.create(...) not supported by " + rawType());
    }

    default List<ColumnType> parameters() {
      throw new UnsupportedOperationException(
          "ColumnType.parameters() not supported by " + rawType());
    }

    @Value.Lazy
    default ColumnType dereference(Keyspace keyspace) {
      return this;
    }

    default boolean isCollection() {
      return false;
    }

    default boolean isUserDefined() {
      return false;
    }

    default boolean isTuple() {
      return false;
    }

    default boolean isList() {
      return false;
    }

    default boolean isMap() {
      return false;
    }

    default boolean isSet() {
      return false;
    }

    default boolean isComplexType() {
      return isParameterized() || isUserDefined();
    }

    ColumnType fieldType(String name);
  }

  public enum Type implements ColumnType {
    Ascii(1, String.class, true, "US-ASCII characters"),
    Bigint(2, Long.class, false, "64-bit signed integer"),
    Blob(3, ByteBuffer.class, false, "Arbitrary bytes"),
    Boolean(4, Boolean.class, false, "True or false"),
    Counter(5, Long.class, false, "64-bit signed integer"),
    Date(
        17,
        LocalDate.class,
        true,
        "32-bit unsigned integer representing the number of days since epoch"),
    Decimal(6, BigDecimal.class, false, "Variable-precision decimal, supports integers and floats"),
    Double(7, Double.class, false, "64-bit IEEE-754 floating point"),
    Duration(21, CqlDuration.class, false, "128 bit encoded duration with nanosecond precision"),
    Float(8, Float.class, false, "32-bit IEEE-754 floating point"),
    Inet(16, InetAddress.class, true, "IP address string in IPv4 or IPv6 format"),
    Int(9, Integer.class, false, "32-bit signed integer"),
    List(32, List.class, false, "listOf(<type>)", "A typed list of values") {
      @Override
      public ColumnType of(ColumnType... types) {
        Preconditions.checkArgument(types.length == 1, "Lists must have one type parameter");
        ImmutableListType.Builder builder = ImmutableListType.builder();
        for (ColumnType colType : types) {
          builder.addParameters(maybeFreezeComplexSubtype(colType));
        }
        return builder.build();
      }

      @Override
      public ParameterizedType.Builder builder() {
        return ImmutableListType.builder();
      }

      @Override
      public boolean isParameterized() {
        return true;
      }

      @Override
      public boolean isCollection() {
        return true;
      }

      @Override
      public boolean isList() {
        return true;
      }
    },
    Map(33, Map.class, false, "mapOf(<type>, <type>)", "A typed map of key value pairs") {
      @Override
      public ColumnType of(ColumnType... types) {
        Preconditions.checkArgument(types.length == 2, "Maps must have two type parameters");
        ImmutableMapType.Builder builder = ImmutableMapType.builder();
        for (ColumnType colType : types) {
          builder.addParameters(maybeFreezeComplexSubtype(colType));
        }
        return builder.build();
      }

      @Override
      public ParameterizedType.Builder builder() {
        return ImmutableMapType.builder();
      }

      @Override
      public boolean isParameterized() {
        return true;
      }

      @Override
      public boolean isCollection() {
        return true;
      }

      @Override
      public boolean isMap() {
        return true;
      }
    },
    Set(34, Set.class, false, "setOf(<type>)", "A typed set of values") {
      @Override
      public ColumnType of(ColumnType... types) {
        Preconditions.checkArgument(types.length == 1, "Sets must have one type parameter");
        ImmutableSetType.Builder builder = ImmutableSetType.builder();
        for (ColumnType colType : types) {
          builder.addParameters(maybeFreezeComplexSubtype(colType));
        }
        return builder.build();
      }

      @Override
      public ParameterizedType.Builder builder() {
        return ImmutableSetType.builder();
      }

      @Override
      public boolean isParameterized() {
        return true;
      }

      @Override
      public boolean isCollection() {
        return true;
      }

      @Override
      public boolean isSet() {
        return true;
      }
    },

    Smallint(19, Short.class, false, "16-bit signed integer"),

    Text(10, String.class, true, "UTF-8 encoded string"),

    Time(
        18,
        LocalTime.class,
        true,
        "Encoded 64-bit signed integers representing the number of nanoseconds since midnight with no corresponding date value"),

    Timestamp(
        11,
        Instant.class,
        true,
        "64-bit signed integer representing the date and time since epoch (January 1 1970 at 00:00:00 GMT) in milliseconds"),

    Timeuuid(
        15,
        UUID.class,
        false,
        "Version 1 UUID; unique identifier that includes a 'conflict-free' timestamp"),

    Tinyint(20, Byte.class, false, "8-bit signed integer"),

    Tuple(
        49,
        TupleValue.class,
        false,
        "tupleOf(<type>...)",
        "Fixed length sequence of elements of different types") {
      @Override
      public ColumnType of(ColumnType... types) {
        ImmutableTupleType.Builder builder = ImmutableTupleType.builder();
        for (ColumnType colType : types) {
          builder.addParameters(maybeFreezeComplexSubtype(colType));
        }
        return builder.build();
      }

      @Override
      public ParameterizedType.Builder builder() {
        return ImmutableTupleType.builder();
      }

      @Override
      public boolean isParameterized() {
        return true;
      }

      @Override
      public boolean isTuple() {
        return true;
      }
    },

    Uuid(12, UUID.class, false, "128 bit universally unique identifier (UUID)"),

    Varchar(13, String.class, true, "UTF-8 encoded string"),

    Varint(14, BigInteger.class, false, "Arbitrary-precision integer"),

    UDT(
        48,
        UdtValue.class,
        false,
        "typeOf(<user_type>)",
        "A user defined type that has been set up previously via 'schema.type(<type_name>).property(<prop_name>, <type>)...create()'") {
      @Override
      public boolean isUserDefined() {
        return true;
      }
    };

    private final int id;
    private final String usage;
    private String description;

    private Class<?> javaType;

    private boolean requiresQuotes;

    private static final Type[] ids;

    static {
      int maxId = -1;
      for (Column.Type t : Column.Type.values()) {
        maxId = Math.max(maxId, t.id());
      }
      ids = new Column.Type[maxId + 1];
      for (Column.Type t : Column.Type.values()) {
        ids[t.id] = t;
      }
    }

    Type(
        final int id, Class<?> javaType, boolean requiresQuotes, String usage, String description) {
      this.id = id;
      this.javaType = javaType;
      this.requiresQuotes = requiresQuotes;
      this.description = description;
      this.usage = usage;
    }

    public static Type fromId(int id) {
      return ids[id];
    }

    Type(int id, Class<?> javaType, boolean requiresQuotes, String description) {
      this(id, javaType, requiresQuotes, null, description);
    }

    @Override
    public Class<?> javaType() {
      return javaType;
    }

    @Override
    public int id() {
      return id;
    }

    @Override
    public Type rawType() {
      return this;
    }

    @Override
    public String cqlDefinition() {
      return name().toLowerCase();
    }

    @Override
    public boolean isFrozen() {
      return false;
    }

    public static Type fromCqlDefinitionOf(String dataTypeName) {
      if (dataTypeName.equalsIgnoreCase(Text.name())) {
        return Varchar;
      }
      for (Type t : values()) {
        if (t.cqlDefinition().replaceAll("'", "").equalsIgnoreCase(dataTypeName)) {
          return t;
        }
      }
      throw new IllegalArgumentException("Cannot retrieve CQL type for '" + dataTypeName + "'");
    }

    public TypeCodec codec() {
      return getCodecs().codec();
    }

    private ColumnUtils.Codecs getCodecs() {
      return ColumnUtils.CODECS.get(this);
    }

    @Override
    public String toString(Object value) {
      Preconditions.checkNotNull(value, "Parameter value cannot be null");
      try {
        String format = codec().format(value);
        if (requiresQuotes) {
          return format.substring(1, format.length() - 1);
        }
        return format;
      } catch (Exception e) {
        if (!codec().accepts(value)) {
          throw new IllegalArgumentException(
              String.format(
                  "Codec '%s' cannot process value of type '%s'",
                  codec().getClass().getSimpleName(), value.getClass().getName()),
              e);
        } else {
          throw new IllegalStateException(e);
        }
      }
    }

    @Override
    public Object fromString(String value) {
      if (requiresQuotes) {
        return codec().parse("'" + value + "'");
      }
      return codec().parse(value);
    }

    public ParameterizedType.Builder builder() {
      throw new UnsupportedOperationException();
    }

    public String description() {
      return description;
    }

    public String usage() {
      return usage == null ? name() : usage;
    }

    @Override
    public ColumnType fieldType(String name) {
      throw new UnsupportedOperationException(
          String.format("'%s' is a raw type and does not have nested columns", this.name()));
    }
  }

  public static class ValidationException extends Exception {
    private final String providedType;
    private final String expectedType;
    private final String expectedCqlType;
    private final String location;
    private final String errorDetails;

    public ValidationException(Class<?> providedType, ColumnType expectedType, String location) {
      this(providedType, expectedType, "", location);
    }

    public ValidationException(ColumnType expectedType, String location) {
      this("<invalid>", expectedType, "", location);
    }

    public ValidationException(
        Class<?> providedType, ColumnType expectedType, String errorMessage, String location) {
      this(providedType.getSimpleName(), expectedType, errorMessage, location);
    }

    private ValidationException(
        String providedType, ColumnType expectedType, String errorMessage, String location) {
      super(
          "Wanted '"
              + expectedType.cqlDefinition()
              + "' but got '"
              + errorMessage
              + "' at location '"
              + location
              + "'");
      this.providedType = providedType;
      this.expectedType = expectedType.javaType().getSimpleName();
      this.expectedCqlType = expectedType.cqlDefinition();
      this.location = location;
      this.errorDetails = "".equals(errorMessage) ? "" : (" Reason: " + errorMessage + ".");
    }

    public String providedType() {
      return providedType;
    }

    public String expectedType() {
      return expectedType;
    }

    public String expectedCqlType() {
      return expectedCqlType;
    }

    public String location() {
      return location;
    }

    public String errorDetails() {
      return errorDetails;
    }
  }

  // To resolve data type from java types
  private static final Map<Class<?>, Type> TYPE_MAPPING =
      ImmutableMap.<Class<?>, Type>builder()
          .put(Date.class, Type.Date)
          .putAll(
              Arrays.stream(Type.values())
                  .filter(
                      r ->
                          !r.isCollection()
                              && r != Counter
                              && r != Ascii
                              && r != Timeuuid
                              && r != Varchar)
                  .collect(Collectors.toMap(r -> r.javaType, r -> r)))
          .build();

  @Nullable
  public abstract ColumnType type();

  public boolean ofTypeText() {
    return ofTypeText(type());
  }

  public static boolean ofTypeText(ColumnType type) {
    return Type.Varchar.equals(type) || Type.Text.equals(type);
  }

  public boolean ofTypeListOrSet() {
    return ofTypeListOrSet(type());
  }

  public static boolean ofTypeListOrSet(ColumnType type) {
    return null != type
        && (Column.Type.Set.equals(type.rawType()) || Column.Type.List.equals(type.rawType()));
  }

  public boolean ofTypeMap() {
    return ofTypeMap(type());
  }

  public static boolean ofTypeMap(ColumnType type) {
    return null != type && Column.Type.Map.equals(type.rawType());
  }

  public boolean isCollection() {
    return ofTypeMap() || ofTypeListOrSet();
  }

  public boolean isFrozenCollection() {
    return isFrozenCollection(type());
  }

  public static boolean isFrozenCollection(ColumnType type) {
    return null != type && type.isFrozen() && type.isCollection();
  }

  public enum Kind {
    PartitionKey,
    Clustering,
    Regular,
    Static;

    public boolean isPrimaryKeyKind() {
      return this == PartitionKey || this == Clustering;
    }
  }

  @Nullable
  public abstract Kind kind();

  @Nullable
  public abstract String keyspace();

  @Nullable
  public abstract String table();

  public Column reference() {
    return reference(name());
  }

  public static Column reference(String name) {
    return ImmutableColumn.builder().name(name).build();
  }

  public static Column create(String name, Kind kind) {
    return ImmutableColumn.builder().name(name).kind(kind).build();
  }

  public static Column create(String name, Column.ColumnType type) {
    return ImmutableColumn.builder().name(name).type(type).kind(Regular).build();
  }

  public static Column create(String name, Kind kind, Column.ColumnType type) {
    return ImmutableColumn.builder().name(name).kind(kind).type(type).build();
  }

  public boolean isPrimaryKeyComponent() {
    if (kind() == null) {
      return false;
    }
    return kind().isPrimaryKeyKind();
  }

  public boolean isPartitionKey() {
    return kind() == Kind.PartitionKey;
  }

  public boolean isClusteringKey() {
    return kind() == Kind.Clustering;
  }

  @Nullable
  @Value.Default
  public Order order() {
    if (kind() == Kind.Clustering) {
      return Order.Asc;
    }
    return null;
  }

  public interface Builder {

    default ImmutableColumn.Builder type(@Nullable Class<?> type) {
      // This is only to support scripting where java types will take precedence over the CQL types
      // Otherwise users will get: No signature of method: ... is applicable for argument types:
      // (java.lang.String, java.lang.Class)
      Type cqlType = TYPE_MAPPING.get(type);
      Preconditions.checkArgument(
          cqlType != null,
          "Tried to set the type for '%s', but this is not a valid CQL type",
          type.getSimpleName());
      return type(cqlType);
    }

    ImmutableColumn.Builder type(@Nullable Column.ColumnType type);
  }

  @Override
  public int compareTo(Column other) {
    if (kind() == other.kind()) {
      return name().compareTo(other.name());
    }
    return kind().compareTo(Objects.requireNonNull(other.kind()));
  }

  private static ColumnType maybeFreezeComplexSubtype(ColumnType type) {
    if (type.isComplexType() && !type.isFrozen()) {
      return type.frozen();
    }
    return type;
  }
}
