package io.stargate.sgv2.restsvc.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for accessing {@link FromProtoValueCodec}s to convert from proto values into externally
 * serializable values.
 */
public class FromProtoValueCodecs {
  private static final JsonNodeFactory jsonNodeFactory =
      JsonNodeFactory.withExactBigDecimals(false);

  private static final BooleanCodec CODEC_BOOLEAN = new BooleanCodec();

  private static final ByteCodec CODEC_BYTE = new ByteCodec();
  private static final IntCodec CODEC_INT = new IntCodec();
  private static final LongCodec CODEC_LONG = new LongCodec();
  private static final ShortCodec CODEC_SHORT = new ShortCodec();
  private static final VarintCodec CODEC_VARINT = new VarintCodec();
  private static final FloatCodec CODEC_FLOAT = new FloatCodec();
  private static final DoubleCodec CODEC_DOUBLE = new DoubleCodec();
  private static final DecimalCodec CODEC_DECIMAL = new DecimalCodec();

  private static final CounterCodec CODEC_COUNTER = new CounterCodec();
  private static final TextCodec CODEC_TEXT = new TextCodec();
  private static final UUIDCodec CODEC_UUID = new UUIDCodec();

  private static final TimestampCodec CODEC_TIMESTAMP = new TimestampCodec();
  private static final DateCodec CODEC_DATE = new DateCodec();
  private static final TimeCodec CODEC_TIME = new TimeCodec();
  private static final InetCodec CODEC_INET = new InetCodec();
  private static final BlobCodec CODEC_BLOB = new BlobCodec();

  public FromProtoValueCodec codecFor(QueryOuterClass.ColumnSpec columnSpec) {
    return codecFor(columnSpec, columnSpec.getType());
  }

  public FromProtoValueCodec codecFor(
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

        // Cases not yet supported:
      case TUPLE:
      case UDT:
        throw new IllegalArgumentException(
            "Can not (yet) create Codec for TypeSpec "
                + type.getSpecCase()
                + " for column '"
                + columnSpec.getName()
                + "'");

        // Invalid cases:
      case SPEC_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Invalid/unsupported ColumnSpec TypeSpec "
                + type.getSpecCase()
                + " for column '"
                + columnSpec.getName()
                + "'");
    }
  }

  protected FromProtoValueCodec basicCodecFor(
      QueryOuterClass.ColumnSpec columnSpec, QueryOuterClass.TypeSpec.Basic basicType) {
    switch (basicType) {
      case CUSTOM:
        throw new IllegalArgumentException(
            "Unsupported ColumnSpec basic type CUSTOM for column: " + columnSpec);

        // // Supported Scalars

        // Supported Scalars: text
      case ASCII:
      case TEXT:
      case VARCHAR:
        return CODEC_TEXT;

        // Supported Scalars: numeric
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
      case FLOAT:
        return CODEC_FLOAT;
      case DOUBLE:
        return CODEC_DOUBLE;
      case DECIMAL:
        return CODEC_DECIMAL;

        // Supported Scalars: other
      case BOOLEAN:
        return CODEC_BOOLEAN;
      case COUNTER:
        // 07-Dec-2021, tatu: Protobuf definitions suggest sint64 value type, BUT SGv1
        //    tests seem to expect JSON String value. So need coercion, alas
        return CODEC_COUNTER;
      case UUID:
      case TIMEUUID:
        return CODEC_UUID;
      case TIMESTAMP:
        return CODEC_TIMESTAMP;
      case DATE:
        return CODEC_DATE;
      case TIME:
        return CODEC_TIME;
      case INET:
        return CODEC_INET;
      case BLOB:
        return CODEC_BLOB;

      case UNRECOGNIZED:
      default:
    }
    throw new IllegalArgumentException("Invalid Basic ColumnSpec value for column: " + columnSpec);
  }

  protected FromProtoValueCodec listCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for LIST column specified as: " + columnSpec);
  }

  protected FromProtoValueCodec mapCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Map mapSpec = columnSpec.getType().getMap();
    return new MapCodec(
        codecFor(columnSpec, mapSpec.getKey()), codecFor(columnSpec, mapSpec.getValue()));
  }

  protected FromProtoValueCodec setCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for SET column specified as: " + columnSpec);
  }

  /* Basic/scalar codec implementations: textual */

  protected static final class TextCodec extends FromProtoValueCodec {
    public static final TextCodec INSTANCE = new TextCodec();

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.string(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(Values.string(value));
    }
  }

  /* Basic/scalar codec implementations: numeric */

  // NOTE! protobuf "getInt()" will return {@code long}; but we will leave that as-is
  // without bothering with casting to avoid creation of unnecessary wrappers.
  protected static final class IntCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.int_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.int_(value));
    }
  }

  protected static final class LongCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.int_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.int_(value));
    }
  }

  protected static final class FloatCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.float_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.float_(value));
    }
  }

  protected static final class DoubleCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.double_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.double_(value));
    }
  }

  // NOTE! protobuf "getInt()" will return {@code long}; but we will leave that as-is
  // without bothering with casting to avoid creation of unnecessary wrappers.
  protected static final class ShortCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.smallint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.smallint(value));
    }
  }

  protected static final class VarintCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.varint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.varint(value));
    }
  }

  protected static final class DecimalCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.decimal(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.decimal(value));
    }
  }

  // NOTE! protobuf "getInt()" will return {@code long}; but we will leave that as-is
  // without bothering with casting to avoid creation of unnecessary wrappers.
  protected static final class ByteCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.tinyint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(Values.tinyint(value));
    }
  }

  // NOTE! Should be able to just use `LongCodec` but SGv1 seems to expect JSON String as
  // return value, and not more logical choice of integral number
  protected static final class CounterCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return String.valueOf(Values.int_(value));
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(String.valueOf(Values.int_(value)));
    }
  }

  /* Basic/scalar codec implementations: other */

  protected static final class BooleanCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.bool(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.booleanNode(Values.bool(value));
    }
  }

  protected static final class UUIDCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.uuid(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.pojoNode(Values.uuid(value));
    }
  }

  protected static final class TimestampCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Instant.ofEpochMilli(value.getInt()).toString();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(Instant.ofEpochMilli(value.getInt()).toString());
    }
  }

  protected static final class DateCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.date(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(Values.date(value).toString());
    }
  }

  protected static final class TimeCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.time(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(Values.time(value).toString());
    }
  }

  protected static final class InetCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return Values.inet(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(Values.inet(value).toString());
    }
  }

  protected static final class BlobCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      // 13-Dec-2021, jsc: Use base-64 encoded value here
      return ByteBufferUtils.toBase64(Values.bytes(value));
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      // 13-Dec-2021, jsc: Use raw bytes here
      return jsonNodeFactory.binaryNode(Values.bytes(value));
    }
  }

  /* Structured codec implementations */

  protected static final class MapCodec extends FromProtoValueCodec {
    private final FromProtoValueCodec keyCodec, valueCodec;

    public MapCodec(FromProtoValueCodec kc, FromProtoValueCodec vc) {
      keyCodec = kc;
      valueCodec = vc;
    }

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      int len = verifyMapLength(coll);
      Map<Object, Object> result = new LinkedHashMap<>(len);
      for (int i = 0; i < len; i += 2) {
        result.put(
            keyCodec.fromProtoValue(coll.getElements(i)),
            valueCodec.fromProtoValue(coll.getElements(i + 1)));
      }
      return result;
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      int len = verifyMapLength(coll);
      ObjectNode map = jsonNodeFactory.objectNode();
      for (int i = 0; i < len; i += 2) {
        map.set(
            keyCodec.jsonNodeFrom(coll.getElements(i)).asText(),
            valueCodec.jsonNodeFrom(coll.getElements(i + 1)));
      }
      return map;
    }

    private int verifyMapLength(QueryOuterClass.Collection mapValue) {
      int len = mapValue.getElementsCount();
      if ((len & 1) != 0) {
        throw new IllegalArgumentException(
            "Illegal Map representation, odd number of Value elements (" + len + ")");
      }
      return len;
    }
  }
}
