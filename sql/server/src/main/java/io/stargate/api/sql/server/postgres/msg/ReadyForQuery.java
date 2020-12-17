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

public class ReadyForQuery extends PGServerMessage {

  private static final ReadyForQuery INSTANCE = new ReadyForQuery();

  private ReadyForQuery() {}

  public static ReadyForQuery instance() {
    return INSTANCE;
  }

  @Override
  public boolean flush() {
    return true;
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte('Z'); // message type
    out.writeInt(5); // message size (without the type byte)
    out.writeByte('I'); // the server is idle (no transaction)
  }
}
