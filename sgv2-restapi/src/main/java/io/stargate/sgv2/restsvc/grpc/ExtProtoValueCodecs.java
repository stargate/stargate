package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

/**
 * Factory for accessing {@link ExtProtoValueCodec}s to convert from proto values into externally
 * serializable values.
 */
public class ExtProtoValueCodecs {
  public ExtProtoValueCodec codecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec type = columnSpec.getType();
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
            "Can not (yet) create Codec for column specified as: " + columnSpec);

        // Invalid cases:
      case SPEC_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Invalid/unsupported ColumnSpec value of " + type + " for column: " + columnSpec);
    }
  }

  protected ExtProtoValueCodec basicCodecFor(
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
        break;
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

  protected ExtProtoValueCodec listCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for LIST column specified as: " + columnSpec);
  }

  protected ExtProtoValueCodec mapCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for MAP column specified as: " + columnSpec);
  }

  protected ExtProtoValueCodec setCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for SET column specified as: " + columnSpec);
  }

  /* Basic codec implementations */

  protected static final class TextCodec extends ExtProtoValueCodec {
    public static final TextCodec INSTANCE = new TextCodec();

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getString();
    }
  }

  protected static final class BooleanCodec extends ExtProtoValueCodec {
    public static final BooleanCodec INSTANCE = new BooleanCodec();

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return value.getBoolean();
    }
  }
}
