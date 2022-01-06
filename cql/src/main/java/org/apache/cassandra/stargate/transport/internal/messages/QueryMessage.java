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
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.QueryOptions;
import org.apache.cassandra.stargate.transport.internal.SchemaAgreement;

/** A CQL query */
public class QueryMessage extends Message.Request {
  public static final Message.Codec<QueryMessage> codec =
      new Message.Codec<QueryMessage>() {
        @Override
        public QueryMessage decode(ByteBuf body, ProtocolVersion version) {
          String query = CBUtil.readLongString(body);
          return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        @Override
        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version) {
          CBUtil.writeLongString(msg.query, dest);
          if (version == ProtocolVersion.V1)
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
          else QueryOptions.codec.encode(msg.options, dest, version);
        }

        @Override
        public int encodedSize(QueryMessage msg, ProtocolVersion version) {
          int size = CBUtil.sizeOfLongString(msg.query);

          if (version == ProtocolVersion.V1) {
            size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
          } else {
            size += QueryOptions.codec.encodedSize(msg.options, version);
          }
          return size;
        }
      };

  public final String query;
  public final QueryOptions options;

  public QueryMessage(String query, QueryOptions options) {
    super(Type.QUERY);
    this.query = query;
    this.options = options;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {
    SimpleStatement statement = new SimpleStatement(query, options.getValues(), options.getNames());
    Persistence.Connection connection = persistenceConnection();
    CompletableFuture<? extends Result> future =
        connection.execute(statement, makeParameters(options), queryStartNanoTime);
    return SchemaAgreement.maybeWaitForAgreement(future, connection).thenApply(ResultMessage::new);
  }

  @Override
  public String toString() {
    return String.format("QUERY %s [pageSize = %d]", query, options.getPageSize());
  }
}
