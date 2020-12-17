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
import java.nio.charset.StandardCharsets;
import org.apache.calcite.sql.SqlKind;

public class CommandComplete extends PGServerMessage {

  private final String response;

  private CommandComplete(String response) {
    this.response = response;
  }

  public static CommandComplete forSet() {
    return new CommandComplete("SET");
  }

  public static CommandComplete forSelect(long rowCount) {
    return new CommandComplete("SELECT " + rowCount);
  }

  public static CommandComplete forDml(SqlKind kind, long rowCount) {
    String pgKind;
    switch (kind) {
      case INSERT:
        pgKind = "INSERT";
        break;
      case UPDATE:
        pgKind = "UPDATE";
        break;
      case DELETE:
        pgKind = "UPDATE";
        break;
      default:
        throw new IllegalArgumentException("Unsupported statement kind: " + kind);
    }

    return new CommandComplete(pgKind + " " + rowCount);
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte('C'); // message type

    int size = 4; // size int32
    byte[] value = response.getBytes(StandardCharsets.UTF_8);
    size += value.length;
    size += 1; // string terminator

    out.writeInt(size);

    out.writeBytes(value);
    out.writeByte(0); // string terminator
  }
}
