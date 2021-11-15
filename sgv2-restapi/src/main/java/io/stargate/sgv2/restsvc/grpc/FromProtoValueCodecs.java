package io.stargate.sgv2.restsvc.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.proto.QueryOuterClass;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for accessing {@link FromProtoValueCodec}s to convert from proto values into externally
 * serializable values.
 */
public class FromProtoValueCodecs {
  static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

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
      case ASCII:
      case TEXT:
      case VARCHAR:
        return TextCodec.INSTANCE;
      case BOOLEAN:
        return BooleanCodec.INSTANCE;
      case BIGINT:
        break;
      case BLOB:
        break;
      case COUNTER:
        break;
      case DECIMAL:
        break;
      case DOUBLE:
        break;
      case FLOAT:
        break;
      case INT:
        break;
      case TIMESTAMP:
        break;
      case UUID:
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
      case SMALLINT:
        break;
      case TINYINT:
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

  /* Basic codec implementations */

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

  protected static final class BooleanCodec extends FromProtoValueCodec {
    public static final BooleanCodec INSTANCE = new BooleanCodec();

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getBoolean();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return jsonNodeFactory.booleanNode(value.getBoolean());
    }
  }

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
