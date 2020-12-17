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
package io.stargate.api.sql.server.postgres.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.stargate.api.sql.server.postgres.msg.ErrorResponse;
import io.stargate.api.sql.server.postgres.msg.MessageParser;
import io.stargate.api.sql.server.postgres.msg.PGClientMessage;
import io.stargate.api.sql.server.postgres.msg.StartupMessage;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDecoder extends ByteToMessageDecoder {

  private static Logger LOG = LoggerFactory.getLogger(MessageDecoder.class);

  private static final long MAX_MSG_SIZE =
      Long.getLong("stargate.sql.postgres.message.max.size", 1_000_000);

  private char messageType = '#'; // undefined
  private long messageSize = -1; // unknown
  private int bodySize = -1; // unknown
  private State state = State.HANDSHAKE; // the type of the first message is implied

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    try {
      switch (state) {
        case HANDSHAKE:
          if (in.readableBytes() < 4) {
            return;
          }

          readMessageSize(in);
          state = State.HANDSHAKE_MESSAGE;
          // fallthrough

        case HANDSHAKE_MESSAGE:
          if (in.readableBytes() < bodySize) {
            return;
          }

          StartupMessage startupMsg = StartupMessage.create(bodySize, in);
          out.add(startupMsg);

          state = startupMsg.startupRequest() ? State.EXPECT_TYPE_CODE : State.HANDSHAKE;
          return;

        case EXPECT_TYPE_CODE:
          if (in.readableBytes() < 1) {
            return;
          }

          short type = in.readUnsignedByte();
          messageType = (char) type;
          state = State.EXPECT_SIZE;
          // fall through

        case EXPECT_SIZE:
          if (in.readableBytes() < 4) {
            return;
          }

          readMessageSize(in);
          state = State.EXPECT_MESSAGE;
          // fall through

        case EXPECT_MESSAGE:
          if (in.readableBytes() < bodySize) {
            return;
          }

          PGClientMessage msg = MessageParser.parse(messageType, bodySize, in);
          out.add(msg);

          state = State.EXPECT_TYPE_CODE;
          return;

        default:
          throw new IllegalStateException("Unsupported state: " + state);
      }
    } catch (Exception ex) {
      LOG.error(
          "Unable to process input. Closing connection. Current state: {}, {}, {}, error: {}",
          state,
          messageType,
          messageSize,
          ex.getMessage(),
          ex);
      // send fatal error message and close connection
      ctx.writeAndFlush(ErrorResponse.fatal(ex)).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private void readMessageSize(ByteBuf in) {
    messageSize = in.readUnsignedInt();
    if (messageSize > MAX_MSG_SIZE) {
      throw new IllegalStateException("Message too long: " + messageSize);
    }

    bodySize = (int) (messageSize - 4); // account for reading the size itself
  }

  private enum State {
    HANDSHAKE,
    HANDSHAKE_MESSAGE,

    EXPECT_TYPE_CODE,
    EXPECT_SIZE,
    EXPECT_MESSAGE,
  }
}
