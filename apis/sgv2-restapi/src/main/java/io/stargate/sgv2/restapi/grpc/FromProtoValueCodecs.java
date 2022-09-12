package io.stargate.sgv2.restapi.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
  private static final DurationCodec CODEC_DURATION = new DurationCodec();
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
      case DURATION:
        return CODEC_DURATION;
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
    QueryOuterClass.TypeSpec.List listSpec = columnSpec.getType().getList();
    return new ListCodec(codecFor(columnSpec, listSpec.getElement()));
  }

  protected FromProtoValueCodec mapCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Map mapSpec = columnSpec.getType().getMap();
    return new MapCodec(
        codecFor(columnSpec, mapSpec.getKey()), codecFor(columnSpec, mapSpec.getValue()));
  }

  protected FromProtoValueCodec setCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Set setSpec = columnSpec.getType().getSet();
    return new SetCodec(codecFor(columnSpec, setSpec.getElement()));
  }

  protected FromProtoValueCodec tupleCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Tuple tupleSpec = columnSpec.getType().getTuple();
    List<FromProtoValueCodec> codecs = new ArrayList<>();
    for (QueryOuterClass.TypeSpec elementSpec : tupleSpec.getElementsList()) {
      codecs.add(codecFor(columnSpec, elementSpec));
    }
    return new TupleCodec(codecs);
  }

  protected FromProtoValueCodec udtCodecFor(QueryOuterClass.ColumnSpec columnSpec) {
    QueryOuterClass.TypeSpec.Udt udtSpec = columnSpec.getType().getUdt();
    Map<String, QueryOuterClass.TypeSpec> fieldSpecs = udtSpec.getFieldsMap();
    Map<String, FromProtoValueCodec> fieldCodecs = new HashMap<>();
    for (Map.Entry<String, QueryOuterClass.TypeSpec> entry : fieldSpecs.entrySet()) {
      final String fieldName = entry.getKey();
      fieldCodecs.put(fieldName, codecFor(columnSpec, entry.getValue()));
    }
    return new UDTCodec(udtSpec.getName(), fieldCodecs);
  }

  /* Basic/scalar codec implementations: textual */

  protected static final class TextCodec extends FromProtoValueCodec {
    public static final TextCodec INSTANCE = new TextCodec();

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.string(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Values.string(value));
    }
  }

  /* Basic/scalar codec implementations: numeric */

  protected static final class IntCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.int_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.int_(value));
    }
  }

  protected static final class LongCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.bigint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.bigint(value));
    }
  }

  protected static final class FloatCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.float_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.float_(value));
    }
  }

  protected static final class DoubleCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.double_(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.double_(value));
    }
  }

  protected static final class ShortCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.smallint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.smallint(value));
    }
  }

  protected static final class VarintCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.varint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.varint(value));
    }
  }

  protected static final class DecimalCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.decimal(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.decimal(value));
    }
  }

  protected static final class ByteCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.tinyint(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.numberNode(Values.tinyint(value));
    }
  }

  // NOTE! Should be able to just use `LongCodec` but SGv1 seems to expect JSON String as
  // return value, and not more logical choice of integral number
  protected static final class CounterCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : String.valueOf(Values.int_(value));
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(String.valueOf(Values.int_(value)));
    }
  }

  /* Basic/scalar codec implementations: other */

  protected static final class BooleanCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.bool(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.booleanNode(Values.bool(value));
    }
  }

  protected static final class UUIDCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.uuid(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.pojoNode(Values.uuid(value));
    }
  }

  protected static final class TimestampCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Instant.ofEpochMilli(value.getInt()).toString();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Instant.ofEpochMilli(value.getInt()).toString());
    }
  }

  protected static final class DateCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.date(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Values.date(value).toString());
    }
  }

  protected static final class TimeCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.time(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Values.time(value).toString());
    }
  }

  protected static final class DurationCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      if (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL) {
        return null;
      }
      // 16-Mar-2022, tatu: Two ways to go; either return CqlDuration and expect
      //    caller to deal with it (requires custom Json serializer), or convert
      //    here. Latter seems easier and safer for now since caller has no need
      //    for actual full type. May be changed later if there is need to retain type.
      return Values.duration(value).toString();
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Values.duration(value).toString());
    }
  }

  protected static final class InetCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.inet(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.textNode(Values.inet(value).toString());
    }
  }

  protected static final class BlobCodec extends FromProtoValueCodec {
    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? null
          : Values.bytes(value);
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      return (value.getInnerCase() == QueryOuterClass.Value.InnerCase.NULL)
          ? jsonNodeFactory.nullNode()
          : jsonNodeFactory.binaryNode(Values.bytes(value));
    }
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Structured codec implementations
  /////////////////////////////////////////////////////////////////////////
   */

  protected static final class ListCodec extends BaseCollectionCodec {
    public ListCodec(FromProtoValueCodec ec) {
      super(ec);
    }

    @Override
    protected Collection<Object> constructCollection(int size) {
      if (size == 0) {
        return Collections.emptyList();
      }
      return new ArrayList<>(size);
    }
  }

  protected static final class SetCodec extends BaseCollectionCodec {
    public SetCodec(FromProtoValueCodec ec) {
      super(ec);
    }

    @Override
    protected Collection<Object> constructCollection(int size) {
      if (size == 0) {
        return Collections.emptySet();
      }
      return new LinkedHashSet<>(size);
    }
  }

  protected abstract static class BaseCollectionCodec extends FromProtoValueCodec {
    private final FromProtoValueCodec elementCodec;

    protected BaseCollectionCodec(FromProtoValueCodec ec) {
      elementCodec = ec;
    }

    protected abstract Collection<Object> constructCollection(int size);

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      final int len = coll.getElementsCount();
      Collection<Object> result = constructCollection(len);
      for (int i = 0; i < len; ++i) {
        result.add(elementCodec.fromProtoValue(coll.getElements(i)));
      }
      return result;
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      ArrayNode result = jsonNodeFactory.arrayNode();
      final int len = coll.getElementsCount();
      for (int i = 0; i < len; ++i) {
        result.add(elementCodec.jsonNodeFrom(coll.getElements(i)));
      }
      return result;
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

  protected static final class TupleCodec extends FromProtoValueCodec {
    private final List<FromProtoValueCodec> elementCodecs;

    public TupleCodec(List<FromProtoValueCodec> elementCodecs) {
      this.elementCodecs = elementCodecs;
    }

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      final int len = verifyTupleLength(coll);
      List<Object> result = new ArrayList<>(len);
      for (int i = 0; i < len; ++i) {
        result.add(elementCodecs.get(i).fromProtoValue(coll.getElements(i)));
      }
      return result;
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      QueryOuterClass.Collection coll = value.getCollection();
      ArrayNode result = jsonNodeFactory.arrayNode();
      final int len = verifyTupleLength(coll);
      for (int i = 0; i < len; ++i) {
        result.add(elementCodecs.get(i).jsonNodeFrom(coll.getElements(i)));
      }
      return result;
    }

    private int verifyTupleLength(QueryOuterClass.Collection tupleValue) {
      int len = tupleValue.getElementsCount();
      if (len != elementCodecs.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Illegal Tuple representation, expected %d values, received %d",
                elementCodecs.size(), len));
      }
      return len;
    }
  }

  protected static final class UDTCodec extends FromProtoValueCodec {
    private final String udtName;
    private final Map<String, FromProtoValueCodec> fieldCodecs;

    public UDTCodec(String udtName, Map<String, FromProtoValueCodec> fieldCodecs) {
      this.udtName = udtName;
      this.fieldCodecs = fieldCodecs;
    }

    @Override
    public Object fromProtoValue(QueryOuterClass.Value value) {
      QueryOuterClass.UdtValue coll = value.getUdt();
      Map<String, QueryOuterClass.Value> encodedFields = coll.getFieldsMap();
      Map<Object, Object> result = new LinkedHashMap<>(encodedFields.size());

      for (Map.Entry<String, QueryOuterClass.Value> entry : encodedFields.entrySet()) {
        final String fieldName = entry.getKey();
        FromProtoValueCodec codec = fieldCodecs.get(fieldName);
        if (codec == null) {
          throw new IllegalArgumentException(
              String.format("UDT '%s' does not have field '%s'", udtName, fieldName));
        }
        result.put(fieldName, codec.fromProtoValue(entry.getValue()));
      }

      return result;
    }

    @Override
    public JsonNode jsonNodeFrom(QueryOuterClass.Value value) {
      QueryOuterClass.UdtValue coll = value.getUdt();
      Map<String, QueryOuterClass.Value> encodedFields = coll.getFieldsMap();
      ObjectNode result = jsonNodeFactory.objectNode();

      for (Map.Entry<String, QueryOuterClass.Value> entry : encodedFields.entrySet()) {
        final String fieldName = entry.getKey();
        FromProtoValueCodec codec = fieldCodecs.get(fieldName);
        if (codec == null) {
          throw new IllegalArgumentException(
              String.format("UDT '%s' does not have field '%s'", udtName, fieldName));
        }
        result.set(fieldName, codec.jsonNodeFrom(entry.getValue()));
      }

      return result;
    }
  }
}
