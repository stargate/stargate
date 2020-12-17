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

public class ParameterStatus extends PGServerMessage {

  private final String name;
  private final String value;

  private ParameterStatus(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public static ParameterStatus of(String name, String value) {
    return new ParameterStatus(name, value);
  }

  public static ParameterStatus serverVersion(String version) {
    return new ParameterStatus("server_version", version);
  }

  public static ParameterStatus timeZone() {
    return new ParameterStatus("TimeZone", "GMT");
  }

  public static ParameterStatus clientEncoding() {
    return new ParameterStatus("client_encoding", "UTF8");
  }

  public static ParameterStatus serverEncoding() {
    return new ParameterStatus("server_encoding", "UTF8");
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte('S'); // message type

    int size = 4; // size int32

    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    size += nameBytes.length;
    size += 1; // string terminator

    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    size += valueBytes.length;
    size += 1; // string terminator

    out.writeInt(size); // message size (without the type byte)
    out.writeBytes(nameBytes);
    out.writeByte(0); // string terminator
    out.writeBytes(valueBytes);
    out.writeByte(0); // string terminator
  }
}
