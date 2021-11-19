package io.stargate.sgv2.restsvc.grpc;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.util.UUID;

public class ToProtoValueCodecs {
  protected static final QueryOuterClass.Value VALUE_FALSE = Values.of(false);
  protected static final QueryOuterClass.Value VALUE_TRUE = Values.of(true);

  protected static final BooleanCodec CODEC_BOOLEAN = new BooleanCodec();
  protected static final TextCodec CODEC_TEXT = new TextCodec();
  protected static final IntCodec CODEC_INT = new IntCodec();
  protected static final LongCodec CODEC_LONG = new LongCodec("LONG");
  protected static final LongCodec CODEC_COUNTER = new LongCodec("COUNTER");

  // Same codecs for UUIDs but for error messages need to create different instances
  protected static final UUIDCodec CODEC_UUID = new UUIDCodec("UUID");
  protected static final UUIDCodec CODEC_TIME_UUID = new UUIDCodec("TIMEUUID");

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

        // Cases not yet supported:
      case TUPLE:
      case UDT:
        throw new IllegalArgumentException(
            "Can not (yet) create Codec for TypeSpec "
                + type.getSpecCase()
                + " for column "
                + columnDesc(columnSpec));

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
      case COUNTER:
        return CODEC_COUNTER; // actually same as LONG

      case UUID:
        return CODEC_UUID;
      case TIMEUUID:
        return CODEC_TIME_UUID;

        // And then not-yet-implemented ones:
      case BLOB:
        break;
      case DECIMAL:
        break;
      case DOUBLE:
        break;
      case FLOAT:
        break;
      case TIMESTAMP:
        break;
      case VARINT:
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
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for LIST column specified as: " + columnSpec);
  }

  protected ToProtoValueCodec mapCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    //    QueryOuterClass.TypeSpec.Map mapSpec = columnSpec.getType().getMap();
    // return new MapCodec(
    //        codecFor(columnSpec, mapSpec.getKey()), codecFor(columnSpec, mapSpec.getValue()));
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for MAP column specified as: " + columnSpec);
  }

  protected ToProtoValueCodec setCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    throw new IllegalArgumentException(
        "Can not (yet) create Codec for SET column specified as: " + columnSpec);
  }

  protected static String columnDesc(QueryOuterClass.ColumnSpec columnSpec) {
    return "'" + columnSpec.getName() + "'";
  }

  /**
   * Base class for all codec implementations, scalar and structured. Mostly used to containe helper
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

  /* Basic/scalar codec implementations */

  protected static final class BooleanCodec extends ToProtoCodecBase {
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

  protected static final class IntCodec extends ToProtoCodecBase {
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

  protected static final class LongCodec extends ToProtoCodecBase {
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

  protected static final class TextCodec extends ToProtoCodecBase {

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

  protected static final class UUIDCodec extends ToProtoCodecBase {
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

  /* Structured type codec implementations */
}
