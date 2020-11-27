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

public class StartupMessage extends PGClientMessage {

  private static final long PROTOCOL_V3 = 0x30000;
  public static final long PROTOCOL_CURRENT = PROTOCOL_V3;

  private static final long SSL_REQUEST = 80877103; // 1234 5679
  private static final long GSS_REQUEST = 80877104; // 1234 5680

  private final boolean sslRequest;
  private final boolean gssRequest;

  public StartupMessage(boolean gssRequest, boolean sslRequest) {
    this.sslRequest = sslRequest;
    this.gssRequest = gssRequest;
  }

  public boolean isSslRequest() {
    return sslRequest;
  }

  public boolean isGssRequest() {
    return gssRequest;
  }

  public boolean startupRequest() {
    return !gssRequest && !sslRequest;
  }

  public static StartupMessage create(int bodySize, ByteBuf bytes) {
    long version = bytes.readUnsignedInt();
    bodySize -= 4;

    if (SSL_REQUEST == version) {
      bytes.skipBytes(bodySize);
      return new StartupMessage(false, true);
    }

    if (GSS_REQUEST == version) {
      bytes.skipBytes(bodySize);
      return new StartupMessage(true, false);
    }

    if (PROTOCOL_V3 != version) {
      bytes.skipBytes(bodySize);
      throw new IllegalStateException("Unsupported protocol version: " + version);
    }

    bytes.skipBytes(bodySize);
    return new StartupMessage(false, false);
  }

  @Override
  public Flowable<PGServerMessage> dispatch(Connection connection) {
    return connection.handshake(this);
  }
}
