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
package io.stargate.api.sql.server.postgres.msg;

import io.netty.buffer.ByteBuf;
import io.stargate.api.sql.server.postgres.FieldFormat;
import io.stargate.api.sql.server.postgres.FieldInfo;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DataRow extends PGServerMessage {

  private final List<byte[]> values = new ArrayList<>();

  private DataRow() {}

  public static DataRow create() {
    return new DataRow();
  }

  public void add(FieldInfo field, Object value) {
    if (field.getFormat() == FieldFormat.TEXT) {
      values.add(value.toString().getBytes(StandardCharsets.UTF_8));
      return;
    }

    throw new IllegalArgumentException("Unsupported format: " + field.getFormat());
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte('D');

    int size = 4; // size int32
    size += 2; // value count int16
    for (byte[] value : values) {
      size += 4; // value length
      size += value.length;
    }

    out.writeInt(size);
    out.writeShort(values.size());

    for (byte[] value : values) {
      out.writeInt(value.length);
      out.writeBytes(value);
    }
  }
}
