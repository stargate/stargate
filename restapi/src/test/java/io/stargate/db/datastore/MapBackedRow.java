/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.datastore;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * A simple DseRow implementation that simply stores data in a {@link Map}. Created via {@link
 * ListBackedResultSet}.
 */
public class MapBackedRow implements Row {
  private final Table table;
  private final Map<String, Object> dataMap;

  public static Row of(Table table, Map<String, Object> dataMap) {
    return new MapBackedRow(table, dataMap);
  }

  private MapBackedRow(Table table, Map<String, Object> data) {
    this.table = table;
    this.dataMap = data;
  }

  @Override
  public List<Column> columns() {
    throw new UnsupportedOperationException(
        "Cannot return columns with simple MapBackedRow implementation.");
  }

  @Nullable
  @Override
  public Object getObject(@NonNull String name) {
    assertThat(table.column(name)).isNotNull();
    return dataMap.get(name);
  }

  @Nullable
  @Override
  public String getString(@NonNull String name) {
    assertThat(table.column(name)).isNotNull();
    return (String) dataMap.get(name);
  }

  @Nullable
  @Override
  public ByteBuffer getBytesUnsafe(@NonNull String name) {
    // Should only be used for building paging name, not for actual values.
    // Assume String keys for now.
    String value = (String) dataMap.get(name);
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public DataType getType(@NonNull String name) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return dataMap.size();
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    throw new UnsupportedOperationException();
  }
}
