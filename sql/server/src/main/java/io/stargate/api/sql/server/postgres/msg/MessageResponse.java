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

public abstract class MessageResponse extends PGServerMessage {

  protected static final char ERROR = 'E';
  protected static final char NOTICE = 'N';

  private final char type;
  private final String severity;
  private final String message;

  protected MessageResponse(char type, String severity, String message) {
    this.type = type;
    this.severity = severity;
    this.message = message;
  }

  @Override
  public boolean flush() {
    return true;
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte(type);

    int size = 4; // size int32

    byte[] sev = severity.getBytes(StandardCharsets.UTF_8);
    size += 1; // field code
    size += sev.length;
    size += 1; // string terminator

    byte[] msg = message.getBytes(StandardCharsets.UTF_8);
    size += 1; // field code
    size += msg.length;
    size += 1; // string terminator

    size += 1; // terminating field code

    out.writeInt(size);

    out.writeByte('S'); // severity
    out.writeBytes(sev);
    out.writeByte(0); // string terminator
    out.writeByte('M'); // message
    out.writeBytes(msg);
    out.writeByte(0); // string terminator
    out.writeByte(0); // terminator field type
  }
}
