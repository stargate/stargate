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
import io.stargate.db.BoundStatement;
import io.stargate.db.Result;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.QueryOptions;
import org.apache.cassandra.stargate.transport.internal.SchemaAgreement;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class ExecuteMessage extends Message.Request {
  public static final Message.Codec<ExecuteMessage> codec =
      new Message.Codec<ExecuteMessage>() {
        @Override
        public ExecuteMessage decode(ByteBuf body, ProtocolVersion version) {
          MD5Digest statementId = MD5Digest.wrap(CBUtil.readBytes(body));

          MD5Digest resultMetadataId = null;
          if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));

          return new ExecuteMessage(
              statementId, resultMetadataId, QueryOptions.codec.decode(body, version));
        }

        @Override
        public void encode(ExecuteMessage msg, ByteBuf dest, ProtocolVersion version) {
          CBUtil.writeBytes(msg.statementId.bytes, dest);

          if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            CBUtil.writeBytes(msg.resultMetadataId.bytes, dest);

          if (version == ProtocolVersion.V1) {
            CBUtil.writeValueList(msg.options.getValues(), dest);
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
          } else {
            QueryOptions.codec.encode(msg.options, dest, version);
          }
        }

        @Override
        public int encodedSize(ExecuteMessage msg, ProtocolVersion version) {
          int size = 0;
          size += CBUtil.sizeOfBytes(msg.statementId.bytes);

          if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            size += CBUtil.sizeOfBytes(msg.resultMetadataId.bytes);

          if (version == ProtocolVersion.V1) {
            size += CBUtil.sizeOfValueList(msg.options.getValues());
            size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
          } else {
            size += QueryOptions.codec.encodedSize(msg.options, version);
          }
          return size;
        }
      };

  public final MD5Digest statementId;
  public final MD5Digest resultMetadataId;
  public final QueryOptions options;

  public ExecuteMessage(MD5Digest statementId, MD5Digest resultMetadataId, QueryOptions options) {
    super(Message.Type.EXECUTE);
    this.statementId = statementId;
    this.options = options;
    this.resultMetadataId = resultMetadataId;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {

    BoundStatement statement =
        new BoundStatement(statementId, options.getValues(), options.getNames());
    CompletableFuture<? extends Result> future =
        persistenceConnection().execute(statement, makeParameters(options), queryStartNanoTime);
    return SchemaAgreement.maybeWaitForAgreement(future, persistenceConnection())
        .thenApply(ResultMessage::new);
  }

  @Override
  public String toString() {
    return String.format(
        "EXECUTE %s with %d values at consistency %s",
        statementId, options.getValues().size(), options.getConsistency());
  }
}
