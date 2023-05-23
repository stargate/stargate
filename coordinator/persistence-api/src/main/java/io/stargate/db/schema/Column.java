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
import static java.lang.String.format;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
    CODEC_REGISTRY = new DefaultCodecRegistry("persistence-codec-registry");
  }

  public enum Order {
    ASC,
    DESC;

    Order reversed() {
      if (this == ASC) {
        return DESC;
      } else {
        return ASC;
      }
    }
  }

  public static final Column TTL = Column.create("[ttl]", Type.Int);
  public static final Column TIMESTAMP = Column.create("[timestamp]", Type.Bigint);

  private static String toCQLString(ColumnType type, Object value) {
    if (value == null) {
      return "null";
    }

    TypeCodec codec = type.codec();
    try {
      return codec.format(type.validate(value, "unknown"));
    } catch (Exception e) {
      if (!codec.accepts(value)) {
        throw new IllegalArgumentException(
            format(
                "Java value %s of type '%s' is not a valid value for CQL type %s",
                value, value.getClass().getName(), type.cqlDefinition()),
            e);
      } else {
        throw new IllegalStateException(e);
      }
    }
  }

  public interface ColumnType extends java.io.Serializable, SchemaHashable {
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

    default String marshalTypeName() {
      throw new UnsupportedOperationException("marshalTypeName() not supported by " + name());
    }

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

    default String toCQLString(Object value) {
      return Column.toCQLString(this, value);
    }

    default String toString(Object value) {
      return toCQLString(value);
    }

    default Object fromString(String value) {
      return codec().parse(value);
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
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

    /**
     * Need a placeholder for (non-dynamic) CompositeType: while not usable via APIs can potentially
     * be created via CQL leading to problems (see {@href
     * https://github.com/stargate/stargate/issues/2144}).
     */
    Composite(
        0, // custom type
        io.stargate.db.schema.CompositeValue.class,
        "org.apache.cassandra.db.marshal.CompositeType",
        true,
        null,
        "Legacy data type"),
    /**
     * Need a placeholder for DynamicCompositeType: while not usable via APIs can potentially be
     * created via CQL leading to problems (see {@href
     * https://github.com/stargate/stargate/issues/2144}).
     */
    DynamicComposite(
        0, // custom type
        io.stargate.db.schema.DynamicCompositeValue.class,
        "org.apache.cassandra.db.marshal.DynamicCompositeType",
        true,
        null,
        "Legacy data type"),
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

    Text(13, String.class, true, "UTF-8 encoded string"),

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

    Varint(14, BigInteger.class, false, "Arbitrary-precision integer"),

    Point(
        0, // custom type
        io.stargate.db.schema.Point.class,
        "org.apache.cassandra.db.marshal.PointType",
        true,
        null,
        "Contains two coordinate values for latitude and longitude"),

    Polygon(
        0, // custom type
        io.stargate.db.schema.Polygon.class,
        "org.apache.cassandra.db.marshal.PolygonType",
        true,
        null,
        "Contains three or more point values forming a polygon"),

    LineString(
        0, // custom type
        io.stargate.db.schema.LineString.class,
        "org.apache.cassandra.db.marshal.LineStringType",
        true,
        null,
        "Contains two or more point values forming a line"),

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
    private final String description;

    private final Class<?> javaType;
    private final String marshalTypeName;

    private final boolean requiresQuotes;

    private static final Type[] ids;

    static {
      int maxId = -1;
      for (Column.Type t : Column.Type.values()) {
        maxId = Math.max(maxId, t.id());
      }
      ids = new Column.Type[maxId + 1];
      for (Column.Type t : Column.Type.values()) {
        if (t.id > 0) {
          ids[t.id] = t;
        }
      }
    }

    Type(
        final int id, Class<?> javaType, boolean requiresQuotes, String usage, String description) {
      this(id, javaType, null, requiresQuotes, usage, description);
    }

    Type(
        final int id,
        Class<?> javaType,
        String marshalTypeName,
        boolean requiresQuotes,
        String usage,
        String description) {
      this.id = id;
      this.marshalTypeName = marshalTypeName;
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
    public int schemaHashCode() {
      return SchemaHashable.enumHashCode(this);
    }

    @Override
    public String marshalTypeName() {
      return marshalTypeName;
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
      if (marshalTypeName != null) {
        return "'" + marshalTypeName + "'";
      }

      return name().toLowerCase();
    }

    @Override
    public boolean isFrozen() {
      return false;
    }

    /**
     * Return the type corresponding to the provided CQL type string.
     *
     * @param keyspace the keyspace within which the provided type should be parsed. This is
     *     necessary to result UDT types.
     * @return the parsed column type.
     * @throws IllegalArgumentException if the provided string is not a valid CQL type
     *     representation or not a known UDT type (in the provided keyspace) or a Custom type
     *     (meaning, as CQL string) as those are not supported by this class yet.
     */
    public static ColumnType fromCqlDefinitionOf(Keyspace keyspace, String dataTypeName) {
      return fromCqlDefinitionOf(keyspace, dataTypeName, true);
    }

    /**
     * Same as {@link #fromCqlDefinitionOf(Keyspace, String)}, but this version doesn't check that
     * UDTs exist in the keyspace. Any type that can be interpreted as a UDT will be returned as a
     * "shallow" {@link UserDefinedType}, that just has its keyspace and name properties set, but no
     * fields. Clients of this method must be prepared to the fact that they might have to
     * "re-resolve" UDTs at a later time.
     */
    public static ColumnType fromCqlDefinitionOf(
        Keyspace keyspace, String dataTypeName, boolean strict) {
      // Parsing CQL types is a tad involved. We "brute-force" it here, but it's neither very
      // efficient, nor very elegant.

      dataTypeName = dataTypeName.trim();
      if (dataTypeName.isEmpty()) {
        throw new IllegalArgumentException("Invalid empty type name");
      }

      if (dataTypeName.charAt(0) == '\'') {
        throw new IllegalArgumentException("Custom types are not supported");
      }

      int lastCharIdx = dataTypeName.length() - 1;
      if (dataTypeName.charAt(0) == '"') {
        // The quote should be terminated and we should have at least 1 character + the quotes,
        if (dataTypeName.charAt(lastCharIdx) != '"' || dataTypeName.length() < 3) {
          throw new IllegalArgumentException(
              "Malformed type name (missing closing quote): " + dataTypeName);
        }
        String udtName = dataTypeName.substring(1, lastCharIdx).replaceAll("\"\"", "\"");
        return findUDTType(keyspace, udtName, strict);
      }

      int paramsIdx = dataTypeName.indexOf('<');
      String baseTypeName =
          paramsIdx < 0 ? dataTypeName : dataTypeName.substring(0, paramsIdx).trim();

      // 11-Jan-2022, tatu: There used to be a call to
      //     `ColumnUtils.isValidUnquotedIdentifier(baseTypeName)`
      //   but it caused issue #1538 and seems like this is not the place for such check.

      Type baseType = parseBaseType(baseTypeName);
      boolean isFrozen = baseType == null && baseTypeName.equalsIgnoreCase("frozen");
      boolean isParameterized = isFrozen || (baseType != null && baseType.isParameterized());
      if (isParameterized) {
        if (paramsIdx < 0) {
          throw new IllegalArgumentException(
              format("Malformed type name: type %s is missing its type parameters", baseTypeName));
        }
        if (dataTypeName.charAt(lastCharIdx) != '>') {
          throw new IllegalArgumentException(
              format(
                  "Malformed type name: parameters for type %s are missing a closing '>'",
                  baseTypeName));
        }
        String paramsString = dataTypeName.substring(paramsIdx + 1, lastCharIdx);
        List<ColumnType> parameters =
            splitAndParseParameters(dataTypeName, keyspace, paramsString, strict);
        if (isFrozen) {
          if (parameters.size() != 1) {
            throw new IllegalArgumentException(
                format(
                    "Malformed type name: frozen takes only 1 parameter, but %d provided",
                    parameters.size()));
          }
          return parameters.get(0).frozen();
        } else {
          return baseType.of(parameters.toArray(new ColumnType[0]));
        }
      } else {
        if (paramsIdx >= 0) {
          throw new IllegalArgumentException(
              format("Malformed type name: type %s cannot have parameters", baseTypeName));
        }
        return baseType == null ? findUDTType(keyspace, baseTypeName, strict) : baseType;
      }
    }

    private static List<ColumnType> splitAndParseParameters(
        String fullTypeName, Keyspace keyspace, String parameters, boolean strict) {
      int openParam = 0;
      int currentStart = 0;
      int idx = currentStart;
      List<ColumnType> parsedParameters = new ArrayList<>();
      while (idx < parameters.length()) {
        switch (parameters.charAt(idx)) {
          case ',':
            // Ignore if we're within a sub-parameter.
            if (openParam == 0) {
              parsedParameters.add(
                  extractParameter(fullTypeName, keyspace, parameters, currentStart, idx, strict));
              currentStart = idx + 1;
            }
            break;
          case '<':
            ++openParam;
            break;
          case '>':
            if (--openParam < 0) {
              throw new IllegalArgumentException(
                  "Malformed type name: " + fullTypeName + " (unmatched closing '>')");
            }
            break;
          case '"':
            idx = findClosingDoubleQuote(fullTypeName, parameters, idx + 1) - 1;
        }
        ++idx;
      }
      parsedParameters.add(
          extractParameter(fullTypeName, keyspace, parameters, currentStart, idx, strict));
      return parsedParameters;
    }

    private static ColumnType extractParameter(
        String fullTypeName,
        Keyspace keyspace,
        String parameters,
        int start,
        int end,
        boolean strict) {
      String parameterStr = parameters.substring(start, end);
      if (parameterStr.isEmpty()) {
        // Recursion actually handle this case, but the error thrown is a bit more cryptic in this
        // context
        throw new IllegalArgumentException("Malformed type name: " + fullTypeName);
      }
      return fromCqlDefinitionOf(keyspace, parameterStr, strict);
    }

    // Returns the index "just after the double quote", so possibly str.length.
    private static int findClosingDoubleQuote(String fullTypeName, String str, int startIdx) {
      int idx = startIdx;
      while (idx < str.length()) {
        if (str.charAt(idx) == '"') {
          // Note: 2 double-quote is a way to escape the double-quote, so move to next first and
          // only exit if it's not a double-quote. Otherwise, continue.
          ++idx;
          if (idx >= str.length() || str.charAt(idx) != '"') {
            return idx;
          }
        }
        ++idx;
      }
      throw new IllegalArgumentException("Malformed type name: " + fullTypeName);
    }

    private static UserDefinedType findUDTType(Keyspace keyspace, String udtName, boolean strict) {
      UserDefinedType udt = keyspace.userDefinedType(udtName);
      if (udt == null) {
        if (strict) {
          throw new IllegalArgumentException(
              format(
                  "Cannot find user type %s int keyspace %s",
                  ColumnUtils.maybeQuote(udtName), keyspace.cqlName()));
        } else {
          udt = ImmutableUserDefinedType.builder().keyspace(keyspace.name()).name(udtName).build();
        }
      }
      return udt;
    }

    private static Type parseBaseType(String str) {
      if ("varchar".equalsIgnoreCase(str)) {
        return Text;
      }
      for (Type t : values()) {
        // Technically, the string "udt" does not correspond to a proper base type in CQL.
        if (t != UDT && t.name().equalsIgnoreCase(str)) {
          return t;
        }
      }
      return null;
    }

    @Override
    public TypeCodec codec() {
      return getCodecs().codec();
    }

    private ColumnUtils.Codecs getCodecs() {
      return ColumnUtils.CODECS.get(this);
    }

    @Override
    public String toString(Object value) {
      // Handling null first, because if the type use quotes, we shouldn't remove them.
      if (value == null) return "null";

      String format = Column.toCQLString(this, value);
      if (requiresQuotes) {
        // Note(Sylvain): I think if we want to remove quotes properly, we should also un-escape
        // any escaped quote. That said, I wonder why we're bothering with removing quotes in the
        // first place: if we want to use the result in a query, we need the quote, and if we need
        // it for debugging, the quote aren't a big deal (and if anything, are more precise).
        // I think this is a bit of a legacy behavior and we should consider to remove that and
        // "merge" this method and the toCQLString(Object) method.
        return format.substring(1, format.length() - 1);
      }
      return format;
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
          format("'%s' is a raw type and does not have nested columns", this.name()));
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
                  .filter(r -> !r.isCollection() && r != Counter && r != Ascii && r != Timeuuid)
                  .collect(Collectors.toMap(r -> r.javaType, r -> r)))
          .build();

  public boolean ofTypeText() {
    return ofTypeText(type());
  }

  public static boolean ofTypeText(ColumnType type) {
    return Type.Text.equals(type);
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
  @Value.Parameter(order = 1)
  public abstract String keyspace();

  @Nullable
  @Value.Parameter(order = 2)
  public abstract String table();

  @Override
  @Value.Parameter(order = 3)
  public abstract String name();

  @Nullable
  @Value.Parameter(order = 4)
  public abstract Kind kind();

  @Nullable
  @Value.Parameter(order = 5)
  public abstract ColumnType type();

  public Column reference() {
    return reference(name());
  }

  public static Column reference(String name) {
    return ImmutableColumn.of(null, null, name, null, null);
  }

  public static Column create(String name, Kind kind) {
    return ImmutableColumn.of(null, null, name, kind, null);
  }

  public static Column create(String name, Column.ColumnType type) {
    return create(name, Regular, type);
  }

  public static Column create(String name, Kind kind, Column.ColumnType type) {
    return ImmutableColumn.of(null, null, name, kind, type);
  }

  public static Column create(String name, Kind kind, Column.ColumnType type, Order order) {
    // keep using builder here, to set order
    // order has calculated default, so can not be a parameter
    if (kind == Kind.Clustering && order == null) {
      order = Order.ASC;
    }
    return ImmutableColumn.builder().name(name).kind(kind).type(type).order(order).build();
  }

  public boolean isPrimaryKeyComponent() {
    Kind kind = kind();
    return kind != null && kind.isPrimaryKeyKind();
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
      return Order.ASC;
    }
    return null;
  }

  @Override
  public int schemaHashCode() {
    return SchemaHashable.combine(
        SchemaHashable.hashCode(name()),
        SchemaHashable.hashCode(type()),
        SchemaHashable.enumHashCode(kind()),
        SchemaHashable.hashCode(keyspace()),
        SchemaHashable.hashCode(table()),
        SchemaHashable.enumHashCode(order()));
  }

  public interface Builder {

    default ImmutableColumn.Builder type(Class<?> type) {
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
    Kind thisKind = kind();
    Kind thatKind = other.kind();
    if (thisKind.equals(thatKind)) {
      return name().compareTo(other.name());
    }
    return thisKind == null ? -1 : thisKind.compareTo(thatKind);
  }

  private static ColumnType maybeFreezeComplexSubtype(ColumnType type) {
    if (type.isComplexType() && !type.isFrozen()) {
      return type.frozen();
    }
    return type;
  }
}
