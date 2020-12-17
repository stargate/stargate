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
import io.reactivex.Flowable;
import io.stargate.api.sql.server.postgres.Connection;

public class Sync extends PGClientMessage {

  private Sync() {}

  public static Sync create(int bodySize, ByteBuf bytes) {
    // no body
    bytes.skipBytes(bodySize); // just in case, bodySize should be 0
    return new Sync();
  }

  @Override
  public Flowable<PGServerMessage> dispatch(Connection connection) {
    return connection.sync().toFlowable();
  }
}
