/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.transport.internal.messages;

import io.netty.buffer.ByteBuf;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.QueryOptions;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BatchMessage extends Message.Request {
  public static final Message.Codec<BatchMessage> codec =
      new Message.Codec<BatchMessage>() {
        @Override
        public BatchMessage decode(ByteBuf body, ProtocolVersion version) {
          byte type = body.readByte();
          if (type < 0 || type > 2) {
            throw new ProtocolException("Invalid BATCH message type " + type);
          }
          int n = body.readUnsignedShort();
          List<Statement> statements = new ArrayList<>(n);
          for (int i = 0; i < n; i++) {
            byte kind = body.readByte();
            if (kind == 0) {
              String query = CBUtil.readLongString(body);
              List<ByteBuffer> values = CBUtil.readValueList(body, version);
              statements.add(new SimpleStatement(query, values, null));
            } else if (kind == 1) {
              MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
              List<ByteBuffer> values = CBUtil.readValueList(body, version);
              statements.add(new BoundStatement(id, values, null));
            } else {
              throw new ProtocolException(
                  "Invalid query kind in BATCH messages. Must be 0 or 1 but got " + kind);
            }
          }
          QueryOptions options = QueryOptions.codec.decode(body, version);

          return new BatchMessage(new Batch(BatchType.fromId(type), statements), options);
        }

        @Override
        public void encode(BatchMessage msg, ByteBuf dest, ProtocolVersion version) {
          int queries = msg.batch.size();

          dest.writeByte(msg.batch.type().id);
          dest.writeShort(queries);

          for (Statement s : msg.batch.statements()) {
            dest.writeByte((byte) (s instanceof SimpleStatement ? 0 : 1));
            if (s instanceof SimpleStatement) {
              CBUtil.writeLongString(((SimpleStatement) s).queryString(), dest);
            } else {
              CBUtil.writeBytes(((BoundStatement) s).preparedId().bytes, dest);
            }

            CBUtil.writeValueList(s.values(), dest);
          }

          if (version.isSmallerThan(ProtocolVersion.V3))
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
          else QueryOptions.codec.encode(msg.options, dest, version);
        }

        @Override
        public int encodedSize(BatchMessage msg, ProtocolVersion version) {
          int size = 3; // type + nb queries
          for (Statement s : msg.batch.statements()) {
            size +=
                1
                    + ((s instanceof SimpleStatement)
                        ? CBUtil.sizeOfLongString(((SimpleStatement) s).queryString())
                        : CBUtil.sizeOfBytes(((BoundStatement) s).preparedId().bytes));

            size += CBUtil.sizeOfValueList(s.values());
          }
          size +=
              version.isSmallerThan(ProtocolVersion.V3)
                  ? CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency())
                  : QueryOptions.codec.encodedSize(msg.options, version);
          return size;
        }
      };

  public final Batch batch;
  public final QueryOptions options;

  public BatchMessage(Batch batch, QueryOptions options) {
    super(Message.Type.BATCH);
    this.batch = batch;
    this.options = options;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {
    CompletableFuture<Result> future =
        persistenceConnection().batch(batch, makeParameters(options), queryStartNanoTime);
    return future.thenApply(ResultMessage::new);
  }

  @Override
  public String toString() {
    return String.format(
        "BATCH of [%s] at consistency %s",
        batch.statements().stream().map(Object::toString).collect(Collectors.joining(", ")),
        options.getConsistency());
  }
}
