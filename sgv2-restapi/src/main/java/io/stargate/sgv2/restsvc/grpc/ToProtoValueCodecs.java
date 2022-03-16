package io.stargate.sgv2.restsvc.grpc;

import io.stargate.core.util.ByteBufferUtils;
import io.stargate.grpc.CqlDuration;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ToProtoValueCodecs {
  protected static final QueryOuterClass.Value VALUE_FALSE = Values.of(false);
  protected static final QueryOuterClass.Value VALUE_TRUE = Values.of(true);

  protected static final BooleanCodec CODEC_BOOLEAN = new BooleanCodec();
  protected static final TextCodec CODEC_TEXT = new TextCodec();
  protected static final IntCodec CODEC_INT = new IntCodec();
  protected static final ShortCodec CODEC_SHORT = new ShortCodec();
  protected static final ByteCodec CODEC_BYTE = new ByteCodec();
  protected static final VarintCodec CODEC_VARINT = new VarintCodec();
  protected static final FloatCodec CODEC_FLOAT = new FloatCodec();
  protected static final DoubleCodec CODEC_DOUBLE = new DoubleCodec();
  protected static final DecimalCodec CODEC_DECIMAL = new DecimalCodec();
  protected static final LongCodec CODEC_LONG = new LongCodec("LONG");
  protected static final LongCodec CODEC_COUNTER = new LongCodec("COUNTER");

  // Same codecs for UUIDs but for error messages need to create different instances
  protected static final UUIDCodec CODEC_UUID = new UUIDCodec("UUID");
  protected static final UUIDCodec CODEC_TIME_UUID = new UUIDCodec("TIMEUUID");

  protected static final TimestampCodec CODEC_TIMESTAMP = new TimestampCodec();
  protected static final DateCodec CODEC_DATE = new DateCodec();
  protected static final TimeCodec CODEC_TIME = new TimeCodec();
  protected static final DurationCodec CODEC_DURATION = new DurationCodec();
  protected static final InetCodec CODEC_INET = new InetCodec();
  protected static final BlobCodec CODEC_BLOB = new BlobCodec();

  public ToProtoValueCodecs() {}

  public ToProtoValueCodec codecFor(QueryOuterClass.ColumnSpec forColumn) {
    return codecFor(forColumn, forColumn.getType());
  }

  public ToProtoValueCodec codecFor(
      QueryOuterClass.ColumnSpec columnSpec, QueryOuterClass.TypeSpec type) {
    switch (type.getSpecCase()) {
      case BASIC:
        return basicCodecFor(columnSpec, type.getBasic());

      case LIST:
        return listCodecFor(columnSpec);
      case MAP:
        return mapCodecFor(columnSpec);
      case SET:
        return setCodecFor(columnSpec);

      case TUPLE:
        return tupleCodecFor(columnSpec);
      case UDT:
        return udtCodecFor(columnSpec);

        // Invalid cases:
      case SPEC_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Invalid/unsupported ColumnSpec TypeSpec "
                + type.getSpecCase()
                + " for column "
                + columnDesc(columnSpec));
    }
  }

  protected ToProtoValueCodec basicCodecFor(
      QueryOuterClass.ColumnSpec columnSpec, QueryOuterClass.TypeSpec.Basic basicType) {
    switch (basicType) {
      case ASCII:
      case TEXT:
      case VARCHAR:
        return CODEC_TEXT;
      case BOOLEAN:
        return CODEC_BOOLEAN;

        // // Numbers:
      case BIGINT:
        return CODEC_LONG;
      case INT:
        return CODEC_INT;
      case SMALLINT:
        return CODEC_SHORT;
      case TINYINT:
        return CODEC_BYTE;
      case VARINT:
        return CODEC_VARINT;
      case COUNTER:
        return CODEC_COUNTER; // actually same as LONG
      case FLOAT:
        return CODEC_FLOAT;
      case DOUBLE:
        return CODEC_DOUBLE;
      case DECIMAL:
        return CODEC_DECIMAL;

      case UUID:
        return CODEC_UUID;
      case TIMEUUID:
        return CODEC_TIME_UUID;
      case TIMESTAMP:
        return CODEC_TIMESTAMP;
      case DATE:
        return CODEC_DATE;
      case TIME:
        return CODEC_TIME;
      case DURATION:
        return CODEC_DURATION;
      case INET:
        return CODEC_INET;
      case BLOB:
        return CODEC_BLOB;

        // As well as ones we don't plan or can't support:
      case CUSTOM:
      case UNRECOGNIZED:
      default:
    }
    throw new IllegalArgumentException(
        "Unsupported Basic ColumnSpec value ("
            + basicType
            + ") for column: "
            + columnDesc(columnSpec));
  }

  protected ToProtoValueCodec listCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.List listSpec = columnSpec.getType().getList();
    return new CollectionCodec(
        "TypeSpec.List", codecFor(columnSpec, listSpec.getElement()), '[', ']');
  }

  protected ToProtoValueCodec mapCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Map mapSpec = columnSpec.getType().getMap();
    return new MapCodec(
        codecFor(columnSpec, mapSpec.getKey()), codecFor(columnSpec, mapSpec.getValue()));
  }

  protected ToProtoValueCodec setCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Set setSpec = columnSpec.getType().getSet();
    return new CollectionCodec(
        "TypeSpec.Set", codecFor(columnSpec, setSpec.getElement()), '{', '}');
  }

  protected ToProtoValueCodec tupleCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Tuple spec = columnSpec.getType().getTuple();
    List<ToProtoValueCodec> codecs = new ArrayList<>();
    for (QueryOuterClass.TypeSpec elementSpec : spec.getElementsList()) {
      codecs.add(codecFor(columnSpec, elementSpec));
    }
    return new TupleCodec(codecs);
  }

  protected ToProtoValueCodec udtCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Udt spec = columnSpec.getType().getUdt();
    Map<String, QueryOuterClass.TypeSpec> fieldSpecs = spec.getFieldsMap();
    Map<String, ToProtoValueCodec> fieldCodecs = new HashMap<>();
    for (Map.Entry<String, QueryOuterClass.TypeSpec> entry : fieldSpecs.entrySet()) {
      final String fieldName = entry.getKey();
      fieldCodecs.put(fieldName, codecFor(columnSpec, entry.getValue()));
    }
    return new UDTCodec(spec.getName(), fieldCodecs);
  }

  protected static String columnDesc(QueryOuterClass.ColumnSpec columnSpec) {
    return "'" + columnSpec.getName() + "'";
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Base classes
  /////////////////////////////////////////////////////////////////////////
   */

  /**
   * Base class for all codec implementations, scalar and structured. Mostly used to contain helper
   * methods used for error reporting.
   */
  protected abstract static class ToProtoCodecBase extends ToProtoValueCodec {
    protected final String grpTypeDesc;

    protected ToProtoCodecBase(String grpTypeDesc) {
      this.grpTypeDesc = grpTypeDesc;
    }

    protected <T> T cannotCoerce(Object javaValue) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot coerce %s into expected protobuf type: %s",
              javaValueDesc(javaValue), grpcTypeDesc()));
    }

    protected <T> T invalidStringValue(String stringValue) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid String value (%s): not valid representation/value for target protobuf type: %s",
              stringValueDesc(stringValue), grpcTypeDesc()));
    }

    protected String grpcTypeDesc() {
      return grpTypeDesc;
    }

    protected static String javaValueDesc(Object javaValue) {
      if (javaValue == null) {
        return "'null'";
      }
      if (javaValue instanceof String) {
        return "String value " + stringValueDesc((String) javaValue);
      }
      if (javaValue instanceof Number) {
        return "Number " + javaValue;
      }
      if (javaValue instanceof Boolean) {
        return String.format("Boolean value (%s)", javaValue);
      }
      return "Java value of type " + javaValue.getClass().getName();
    }

    protected static String stringValueDesc(String value) {
      // !!! TODO: escape non-printable
      return "'" + value + "'";
    }
  }

  protected abstract static class ToProtoScalarCodecBase extends ToProtoCodecBase {
    protected ToProtoScalarCodecBase(String grpcTypeDesc) {
      super(grpcTypeDesc);
    }

    @Override
    public ToProtoValueCodec getValueCodec() {
      return null;
    }

    @Override
    public ToProtoValueCodec getKeyCodec() {
      return null;
    }
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Basic/scalar codec implementations
  /////////////////////////////////////////////////////////////////////////
   */

  protected static final class BooleanCodec extends ToProtoScalarCodecBase {
    public BooleanCodec() {
      super("TypeSpec.Basic.BOOLEAN");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Boolean) {
        return Values.of((Boolean) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      // First check case-sensitive; common case and much faster
      if ("true".equals(value)) {
        return VALUE_TRUE;
      }
      if ("false".equals(value)) {
        return VALUE_FALSE;
      }
      // And only then case-insensitive
      if (value.equalsIgnoreCase("true")) {
        return VALUE_TRUE;
      }
      if (value.equalsIgnoreCase("false")) {
        return VALUE_FALSE;
      }
      return invalidStringValue(value);
    }
  }

  protected static final class IntCodec extends ToProtoScalarCodecBase {
    public IntCodec() {
      super("TypeSpec.Basic.INT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      int v;
      if (value instanceof Integer) {
        v = ((Integer) value).intValue();
      } else if (value instanceof Number) {
        // !!! TODO: bounds checks
        v = ((Number) value).intValue();
      } else {
        return cannotCoerce(value);
      }
      return Values.of(v);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Integer.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class ShortCodec extends ToProtoScalarCodecBase {
    public ShortCodec() {
      super("TypeSpec.Basic.SMALLINT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Number) {
        // !!! TODO: bounds checks
        // Note: Java defaults to treating as Integer, this handles that case
        return Values.of(((Number) value).shortValue());
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Short.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class ByteCodec extends ToProtoScalarCodecBase {
    public ByteCodec() {
      super("TypeSpec.Basic.TINYINT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Number) {
        // !!! TODO: bounds checks
        // Note: Java defaults to treating as Integer, this handles that case
        return Values.of(((Number) value).byteValue());
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Byte.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class LongCodec extends ToProtoScalarCodecBase {
    public LongCodec(String numberType) {
      super("TypeSpec.Basic." + numberType);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      long v;
      if (value instanceof Long) {
        v = ((Long) value).longValue();
      } else if (value instanceof Number) {
        // !!! TODO: bounds checks
        v = ((Number) value).longValue();
      } else {
        return cannotCoerce(value);
      }
      return Values.of(v);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Long.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class VarintCodec extends ToProtoScalarCodecBase {
    public VarintCodec() {
      super("TypeSpec.Basic.VARINT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof BigInteger) {
        return Values.of((BigInteger) value);
      } else if (value instanceof Number) {
        Number n = (Number) value;
        return Values.of(new BigInteger(n.toString()));
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(new BigInteger(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class FloatCodec extends ToProtoScalarCodecBase {
    public FloatCodec() {
      super("TypeSpec.Basic.FLOAT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Number) {
        // !!! TODO: bounds checks
        // Note: Java defaults to treating as Double, this handles that case
        return Values.of(((Number) value).floatValue());
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Float.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class DoubleCodec extends ToProtoScalarCodecBase {
    public DoubleCodec() {
      super("TypeSpec.Basic.DOUBLE");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Double) {
        return Values.of((Double) value);
      } else if (value instanceof Number) {
        // !!! TODO: bounds checks
        return Values.of(((Number) value).doubleValue());
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(Double.valueOf(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class DecimalCodec extends ToProtoScalarCodecBase {
    public DecimalCodec() {
      super("TypeSpec.Basic.DECIMAL");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof BigDecimal) {
        return Values.of((BigDecimal) value);
      } else if (value instanceof Number) {
        Number n = (Number) value;
        return Values.of(new BigDecimal(n.toString()));
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(new BigDecimal(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class TextCodec extends ToProtoScalarCodecBase {

    public TextCodec() {
      super("TypeSpec.Basic.TEXT");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      return Values.of(String.valueOf(value));
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      return Values.of(value);
    }
  }

  protected static final class UUIDCodec extends ToProtoScalarCodecBase {
    public UUIDCodec(String typeDesc) {
      super("TypeSpec.Basic." + typeDesc);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      // Could also support Binary, in theory, but JSON won't expose as such:
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      // 16-Nov-2021, tatu: Should probably optimize, as per:
      //
      // https://cowtowncoder.medium.com/measuring-performance-of-java-uuid-fromstring-or-lack-thereof-d16a910fa32a
      //
      //  but first let's make it work, then make it fast
      try {
        return Values.of(UUID.fromString(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class InetCodec extends ToProtoScalarCodecBase {
    public InetCodec() {
      super("TypeSpec.Basic.INET");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      } else if (value instanceof InetAddress) {
        return Values.of((InetAddress) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(InetAddress.getByName(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      } catch (UnknownHostException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class TimestampCodec extends ToProtoScalarCodecBase {
    public TimestampCodec() {
      super("TypeSpec.Basic.TIMESTAMP");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        // TODO: this implementation requires full date/time specification including timezone
        // we could support more flexibility in format as requested in
        // https://github.com/stargate/stargate/issues/839
        return Values.of(Instant.parse(value).toEpochMilli());
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class DateCodec extends ToProtoScalarCodecBase {
    public DateCodec() {
      super("TypeSpec.Basic.DATE");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(LocalDate.parse(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class TimeCodec extends ToProtoScalarCodecBase {
    public TimeCodec() {
      super("TypeSpec.Basic.TIME");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(LocalTime.parse(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class DurationCodec extends ToProtoScalarCodecBase {
    public DurationCodec() {
      super("TypeSpec.Basic.DURATION");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      try {
        return Values.of(CqlDuration.from(value));
      } catch (IllegalArgumentException e) {
        return invalidStringValue(value);
      }
    }
  }

  protected static final class BlobCodec extends ToProtoScalarCodecBase {
    public BlobCodec() {
      super("TypeSpec.Basic.BLOB");
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      // Since we are getting this JSON or path expression, it must be a
      // Base64-encoded String:
      if (value instanceof String) {
        return protoValueFromStringified((String) value);
      }
      // But just for sake of completeness I guess we can accept other theoritcal
      // possibilities
      if (value instanceof byte[]) {
        return Values.of((byte[]) value);
      } else if (value instanceof ByteBuffer) {
        return Values.of((ByteBuffer) value);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      return Values.of(ByteBufferUtils.fromBase64(value));
    }
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Structured type codec implementations
  /////////////////////////////////////////////////////////////////////////
   */

  /**
   * Note: since internally gRPC uses Collection to represent Lists and Sets alike, all we need is
   * one codec.
   */
  protected static final class CollectionCodec extends ToProtoCodecBase {
    protected final ToProtoValueCodec elementCodec;
    protected final char openingBrace;
    protected final char closingBrace;

    public CollectionCodec(
        String id, ToProtoValueCodec elementCodec, char openingBrace, char closingBrace) {
      super(id);
      this.elementCodec = elementCodec;
      this.openingBrace = openingBrace;
      this.closingBrace = closingBrace;
    }

    @Override
    public ToProtoValueCodec getKeyCodec() {
      return null;
    }

    @Override
    public ToProtoValueCodec getValueCodec() {
      return elementCodec;
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object javaValue) {
      if (javaValue instanceof Collection<?>) {
        List<QueryOuterClass.Value> elements = new ArrayList<>();
        for (Object value : (Collection<?>) javaValue) {
          elements.add(elementCodec.protoValueFromStrictlyTyped(value));
        }
        return Values.of(elements);
      }
      return cannotCoerce(javaValue);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      List<QueryOuterClass.Value> elements = new ArrayList<>();
      StringifiedValueUtil.decodeStringifiedCollection(
          value, elementCodec, elements, openingBrace, closingBrace);
      return Values.of(elements);
    }
  }

  protected static final class MapCodec extends ToProtoCodecBase {
    private final ToProtoValueCodec keyCodec, valueCodec;

    public MapCodec(ToProtoValueCodec keyCodec, ToProtoValueCodec valueCodec) {
      super("TypeSpec.Map");
      this.keyCodec = keyCodec;
      this.valueCodec = valueCodec;
    }

    @Override
    public ToProtoValueCodec getKeyCodec() {
      return keyCodec;
    }

    @Override
    public ToProtoValueCodec getValueCodec() {
      return valueCodec;
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object mapValue) {
      if (mapValue instanceof Map<?, ?>) {
        // Maps are actually encoded as Collections where keys and values are interleaved
        List<QueryOuterClass.Value> elements = new ArrayList<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) mapValue).entrySet()) {
          elements.add(keyCodec.protoValueFromStrictlyTyped(entry.getKey()));
          elements.add(valueCodec.protoValueFromStrictlyTyped(entry.getValue()));
        }
        return Values.of(elements);
      }
      return cannotCoerce(mapValue);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      List<QueryOuterClass.Value> elements = new ArrayList<>();
      StringifiedValueUtil.decodeStringifiedMap(value, keyCodec, valueCodec, elements);
      return Values.of(elements);
    }
  }

  protected static final class TupleCodec extends ToProtoCodecBase {
    private final List<ToProtoValueCodec> elementCodecs;

    public TupleCodec(List<ToProtoValueCodec> elementCodecs) {
      super("TypeSpec.Tuple");
      this.elementCodecs = elementCodecs;
    }

    @Override
    public ToProtoValueCodec getKeyCodec() {
      return null;
    }

    @Override
    public ToProtoValueCodec getValueCodec() {
      return null;
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object value) {
      if (value instanceof Collection<?>) {
        Collection<Object> collectionValue = (Collection<Object>) value;
        final int len = collectionValue.size();
        if (len != elementCodecs.size()) {
          throw new IllegalArgumentException(
              String.format("Tuple expected %d values, got %d", elementCodecs.size(), len));
        }
        int i = 0;
        List<QueryOuterClass.Value> decoded = new ArrayList<>();

        for (Object rawElement : collectionValue) {
          final ToProtoValueCodec codec = elementCodecs.get(i++);
          decoded.add(codec.protoValueFromStrictlyTyped(rawElement));
        }
        // Tuples are essentially Collections when transported over gRPC
        return Values.of(decoded);
      }
      return cannotCoerce(value);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      List<QueryOuterClass.Value> decoded = new ArrayList<>(elementCodecs.size());
      StringifiedValueUtil.decodeStringifiedTuple(value, elementCodecs, decoded);
      return Values.of(decoded);
    }
  }

  protected static final class UDTCodec extends ToProtoCodecBase {
    private final String udtName;
    private final Map<String, ToProtoValueCodec> fieldCodecs;

    public UDTCodec(String udtName, Map<String, ToProtoValueCodec> fieldCodecs) {
      super("TypeSpec.UDT." + udtName);
      this.udtName = udtName;
      this.fieldCodecs = fieldCodecs;
    }

    @Override
    public ToProtoValueCodec getKeyCodec() {
      return null;
    }

    @Override
    public ToProtoValueCodec getValueCodec() {
      return null;
    }

    @Override
    public QueryOuterClass.Value protoValueFromStrictlyTyped(Object mapValue) {
      if (mapValue instanceof Map<?, ?>) {
        Map<String, QueryOuterClass.Value> decoded = new HashMap<>();
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) mapValue).entrySet()) {
          final String fieldName = entry.getKey();
          ToProtoValueCodec codec = fieldCodecs.get(fieldName);
          if (codec == null) {
            throw new IllegalArgumentException(
                String.format("UDT '%s' does not have field '%s'", udtName, fieldName));
          }
          decoded.put(fieldName, codec.protoValueFromStrictlyTyped(entry.getValue()));
        }
        return Values.udtOf(decoded);
      }
      return cannotCoerce(mapValue);
    }

    @Override
    public QueryOuterClass.Value protoValueFromStringified(String value) {
      Map<String, QueryOuterClass.Value> decoded = new LinkedHashMap<>();
      StringifiedValueUtil.decodeStringifiedUDT(value, fieldCodecs, udtName, decoded);
      return Values.udtOf(decoded);
    }
  }
}
