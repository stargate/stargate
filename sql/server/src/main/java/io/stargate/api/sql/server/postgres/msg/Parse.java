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

public class Parse extends ExtendedQueryMessage {

  private final String name;
  private final String sql;
  private final int[] paramTypes;

  private Parse(String name, String sql, int[] paramTypes) {
    this.name = name;
    this.sql = sql;
    this.paramTypes = paramTypes;
  }

  public static Parse create(int bodySize, ByteBuf bytes) {
    String name = readString(bytes);
    String sql = readString(bytes);

    int numParams = bytes.readUnsignedShort();
    int[] paramTypes = new int[numParams];
    for (int i = 0; i < paramTypes.length; i++) {
      paramTypes[i] = bytes.readInt();
    }

    return new Parse(name, sql, paramTypes);
  }

  @Override
  public Flowable<PGServerMessage> process(Connection connection) {
    return connection.prepare(this).toFlowable();
  }

  public String getName() {
    return name;
  }

  public String getSql() {
    return sql;
  }
}
