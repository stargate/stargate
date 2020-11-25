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

/**
 * <a href="https://www.postgresql.org/docs/13/protocol-message-types.html">PostgreSQL Protocol
 * Reference</a>
 */
public class MessageParser {

  public static PGClientMessage parse(char type, int bodySize, ByteBuf bytes) {
    int idx0 = bytes.readerIndex();

    PGClientMessage msg;
    switch (type) {
      case 'B':
        msg = Bind.create(bodySize, bytes);
        break;
      case 'D':
        msg = Describe.create(bodySize, bytes);
        break;
      case 'E':
        msg = Execute.create(bodySize, bytes);
        break;
      case 'H':
        msg = Flush.create(bodySize, bytes);
        break;
      case 'P':
        msg = Parse.create(bodySize, bytes);
        break;
      case 'Q':
        msg = Query.create(bodySize, bytes);
        break;
      case 'S':
        msg = Sync.create(bodySize, bytes);
        break;

      default:
        throw new IllegalStateException("Unsupported message type: " + type);
    }

    int idx1 = bytes.readerIndex();
    int toSkip = bodySize - (idx1 - idx0);
    bytes.skipBytes(toSkip);

    return msg;
  }
}
