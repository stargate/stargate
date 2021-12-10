package io.stargate.sgv2.restsvc.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.uuid.impl.UUIDUtil;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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
  private static final DoubleCodec CODEC_DOUBLE = new DoubleCodec();
  private static final DecimalCodec CODEC_DECIMAL = new DecimalCodec();

  private static final CounterCodec CODEC_COUNTER = new CounterCodec();
  private static final TextCodec CODEC_TEXT = new TextCodec();
  private static final UUIDCodec CODEC_UUID = new UUIDCodec();

  private static final TimestampCodec CODEC_TIMESTAMP = new TimestampCodec();

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
        return CODEC_UUID;
      case TIMESTAMP:
        return CODEC_TIMESTAMP;

        // // // To Be Implemented:

      case BLOB:
        break;
      case FLOAT:
        break;
      case VARINT:
        break;
      case TIMEUUID:
        break;
      case INET:
        break;
      case DATE:
        break;
      case TIME:
        break;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            "Invalid Basic ColumnSpec value for column: " + columnSpec);
    }
    throw new IllegalArgumentException(
        "Unsupported Basic ColumnSpec value for column: " + columnSpec);
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
      return value.getString();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(value.getString());
    }
  }

  /* Basic/scalar codec implementations: numeric */

  // NOTE! protobuf "getInt()" will return {@code long}; but we will leave that as-is
  // without bothering with casting to avoid creation of unnecessary wrappers.
  protected static final class IntCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getInt();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(value.getInt());
    }
  }

  protected static final class LongCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getInt();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(value.getInt());
    }
  }

  protected static final class DoubleCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getDouble();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(value.getDouble());
    }
  }

  // NOTE! protobuf "getInt()" will return {@code long}; but we will leave that as-is
  // without bothering with casting to avoid creation of unnecessary wrappers.
  protected static final class ShortCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getInt();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(value.getInt());
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
      return value.getInt();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.numberNode(value.getInt());
    }
  }

  // NOTE! Should be able to just use `LongCodec` but SGv1 seems to expect JSON String as
  // return value, and not more logical choice of integral number
  protected static final class CounterCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return String.valueOf(value.getInt());
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.textNode(String.valueOf(value.getInt()));
    }
  }

  /* Basic/scalar codec implementations: other */

  protected static final class BooleanCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getBoolean();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.booleanNode(value.getBoolean());
    }
  }

  protected static final class UUIDCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      // 19-Nov-2021, tatu: Two choices here, both of which JAX-RS can deal with it:
      //   (a) just return UUID as-is; (b) convert to String.
      //   Going with (a) for now
      return uuidFrom(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      // 19-Nov-2021, tatu: As with above, go with "native"/embedded
      return jsonNodeFactory.pojoNode(uuidFrom(value));
    }

    private UUID uuidFrom(QueryOuterClass.Value value) {
      // Must be careful not to get bytes for Message (has 2 type bytes) but Value within
      byte[] bs = value.getUuid().getValue().toByteArray();
      if (bs.length != 16) {
        throw new IllegalArgumentException(
            "Wrong length for UUID encoding: expected 16, was: " + bs.length);
      }
      return UUIDUtil.uuid(bs);
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
