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
package io.stargate.api.sql.server.postgres;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.stargate.api.sql.server.postgres.msg.ErrorResponse;
import io.stargate.api.sql.server.postgres.msg.PGClientMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDispatcher extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

  private final Connection connection;

  public MessageDispatcher(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof PGClientMessage) {
      connection.enqueue((PGClientMessage) msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    // send fatal error message and close connection
    LOG.error(
        "Exception in PostgreSQL protocol handler, closing connection: {}",
        cause.toString(),
        cause);
    ctx.writeAndFlush(ErrorResponse.fatal(cause)).addListener(ChannelFutureListener.CLOSE);
  }
}
