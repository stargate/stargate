package io.stargate.grpc.codec.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

public class CollectionCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull QueryOuterClass.Value value, @NonNull Column.ColumnType type) {
    if (!value.hasCollection()) {
      throw new IllegalArgumentException("Expected collection type");
    }
    assert type.isCollection();

    Collection collection = value.getCollection();

    Column.ColumnType elementType = type.parameters().get(0);
    ValueCodec elementCodec = ValueCodecs.get(elementType.rawType());

    int elementCount = collection.getElementsCount();
    ByteBuffer[] encodedElements = new ByteBuffer[elementCount];
    int toAllocate = 4;
    for (int i = 0; i < elementCount; ++i) {
      ByteBuffer encodedElement =
          ValueCodec.encodeValue(elementCodec, collection.getElements(i), elementType);
      if (encodedElement == null) {
        throw new IllegalArgumentException(
            String.format("null is not supported inside %ss", type.rawType().cqlDefinition()));
      }
      encodedElements[i] = encodedElement;
      toAllocate += 4 + encodedElement.remaining();
    }

    ByteBuffer result = ByteBuffer.allocate(toAllocate);
    result.putInt(elementCount);
    return encodeValues(encodedElements, result);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    Collection.Builder builder = Collection.newBuilder();

    Column.ColumnType elementType = type.parameters().get(0);
    ValueCodec elementCodec = ValueCodecs.get(elementType.rawType());

    ByteBuffer input = bytes.duplicate();
    int elementCount = input.getInt();
    for (int i = 0; i < elementCount; i++) {
      builder.addElements(decodeValue(input, elementCodec, elementType));
    }

    return Value.newBuilder().setCollection(builder).build();
  }
}
