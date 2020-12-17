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

public class Describe extends ExtendedQueryMessage {

  private final boolean statement;
  private final String name;

  private Describe(boolean statement, String name) {
    this.statement = statement;
    this.name = name;
  }

  public boolean isStatement() {
    return statement;
  }

  public String getName() {
    return name;
  }

  public static Describe create(int bodySize, ByteBuf bytes) {
    char target = (char) bytes.readByte();
    String name = readString(bytes);

    if (target == 'S') {
      return new Describe(true, name);
    } else if (target == 'P') { // portal
      return new Describe(false, name);
    } else {
      throw new IllegalStateException("Unsupported Describe target: " + target);
    }
  }

  @Override
  public Flowable<PGServerMessage> process(Connection connection) {
    if (isStatement()) {
      throw new UnsupportedOperationException("Describe is not supported for statements");
    }

    return connection.describePortal(getName());
  }
}
