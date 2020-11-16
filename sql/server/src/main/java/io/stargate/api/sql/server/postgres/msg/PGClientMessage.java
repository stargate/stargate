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
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PGClientMessage {
  private static final Logger LOG = LoggerFactory.getLogger(PGClientMessage.class);

  public final Flowable<PGServerMessage> process(Connection connection) {
    if (skip(connection)) {
      LOG.info("skipped: " + getClass().getSimpleName());
      return Flowable.empty();
    }

    return dispatch(connection);
  }

  protected boolean skip(Connection connection) {
    return connection.hasErrors();
  }

  protected Flowable<PGServerMessage> dispatch(Connection connection) {
    throw new IllegalStateException("Dispatch not implemented in " + getClass());
  }

  protected static String readString(ByteBuf in) {
    in.markReaderIndex();
    int size = 0;
    while (in.readByte() != 0) {
      size++;
    }
    in.resetReaderIndex();

    byte[] value = new byte[size];
    in.readBytes(value);
    in.readByte(); // string terminator

    return new String(value, StandardCharsets.UTF_8);
  }
}
