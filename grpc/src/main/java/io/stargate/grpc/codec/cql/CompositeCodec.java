package io.stargate.grpc.codec.cql;

import io.stargate.db.schema.Column;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

public abstract class CompositeCodec implements ValueCodec {

  protected Value decodeValue(ByteBuffer bytes, ValueCodec codec, Column.ColumnType type) {
    Value element;
    int elementSize = bytes.getInt();
    if (elementSize < 0) {
      element = Values.NULL;
    } else {
      ByteBuffer encodedElement = bytes.slice();
      encodedElement.limit(elementSize);
      element = ValueCodec.decodeValue(codec, encodedElement, type);
      bytes.position(bytes.position() + elementSize);
    }
    return element;
  }

  protected ByteBuffer encodeValues(ByteBuffer[] values, ByteBuffer result) {
    for (ByteBuffer value : values) {
      if (value == null) {
        result.putInt(-1);
      } else {
        result.putInt(value.remaining());
        result.put(value);
      }
    }
    result.flip();
    return result;
  }
}
