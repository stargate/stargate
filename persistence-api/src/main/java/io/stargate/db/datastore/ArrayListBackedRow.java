package io.stargate.db.datastore;

import static java.lang.String.format;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ArrayListBackedRow implements Row {

  private final List<Column> columns;
  private final List<ByteBuffer> values;
  private final ProtocolVersion protocolVersion;

  public ArrayListBackedRow(
      List<Column> columns, List<ByteBuffer> values, ProtocolVersion protocolVersion) {
    assert columns.size() == values.size();
    this.columns = columns;
    this.values = values;
    this.protocolVersion = protocolVersion;
  }

  @Override
  public List<Column> columns() {
    return columns;
  }

  private void checkIndex(int index) {
    if (index < 0 || index >= columns.size()) {
      throw new IndexOutOfBoundsException(
          format("Index %d is out of bounds: the row has %d columns", index, columns.size()));
    }
  }

  @Override
  public int firstIndexOf(@Nonnull String column) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(column)) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        format("Column '%s' is not defined in the Row's metadata.", column));
  }

  @Nonnull
  @Override
  public DataType getType(@Nonnull String column) {
    return getType(firstIndexOf(column));
  }

  @Nonnull
  @Override
  public DataType getType(int i) {
    checkIndex(i);
    return columns.get(i).type().codec().getCqlType();
  }

  @Nullable
  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    checkIndex(i);
    return values.get(i);
  }

  @Override
  public int size() {
    return values.size();
  }

  @Nonnull
  @Override
  public CodecRegistry codecRegistry() {
    return Column.CODEC_REGISTRY;
  }

  @Nonnull
  @Override
  public ProtocolVersion protocolVersion() {
    return protocolVersion;
  }

  @Override
  public String toString() {
    return columns().stream()
        .map(c -> format("%s=%s", c.name(), getObject(c.name())))
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
