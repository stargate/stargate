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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.stargate.cql3.DefaultQueryOptions;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.utils.MD5Digest;

import io.stargate.db.BatchType;
import io.stargate.db.Persistence;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.netty.buffer.ByteBuf;

public class BatchMessage extends Message.Request
{
    public static final Message.Codec<BatchMessage> codec = new Message.Codec<BatchMessage>()
    {
        public BatchMessage decode(ByteBuf body, ProtocolVersion version)
        {
            byte type = body.readByte();
            int n = body.readUnsignedShort();
            List<Object> queryOrIds = new ArrayList<>(n);
            List<List<ByteBuffer>> variables = new ArrayList<>(n);
            for (int i = 0; i < n; i++)
            {
                byte kind = body.readByte();
                if (kind == 0)
                    queryOrIds.add(CBUtil.readLongString(body));
                else if (kind == 1)
                    queryOrIds.add(MD5Digest.wrap(CBUtil.readBytes(body)));
                else
                    throw new ProtocolException("Invalid query kind in BATCH messages. Must be 0 or 1 but got " + kind);
                variables.add(CBUtil.readValueList(body, version));
            }
            QueryOptions options = DefaultQueryOptions.codec.decode(body, version);

            return new BatchMessage(type, queryOrIds, variables, options);
        }

        public void encode(BatchMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            int queries = msg.queryOrIdList.size();

            dest.writeByte(msg.batchType);
            dest.writeShort(queries);

            for (int i = 0; i < queries; i++)
            {
                Object q = msg.queryOrIdList.get(i);
                dest.writeByte((byte)(q instanceof String ? 0 : 1));
                if (q instanceof String)
                    CBUtil.writeLongString((String)q, dest);
                else
                    CBUtil.writeBytes(((MD5Digest)q).bytes, dest);

                CBUtil.writeValueList(msg.values.get(i), dest);
            }

            if (version.isSmallerThan(ProtocolVersion.V3))
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                DefaultQueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(BatchMessage msg, ProtocolVersion version)
        {
            int size = 3; // type + nb queries
            for (int i = 0; i < msg.queryOrIdList.size(); i++)
            {
                Object q = msg.queryOrIdList.get(i);
                size += 1 + (q instanceof String
                             ? CBUtil.sizeOfLongString((String)q)
                             : CBUtil.sizeOfBytes(((MD5Digest)q).bytes));

                size += CBUtil.sizeOfValueList(msg.values.get(i));
            }
            size += version.isSmallerThan(ProtocolVersion.V3)
                  ? CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency())
                  : DefaultQueryOptions.codec.encodedSize(msg.options, version);
            return size;
        }
    };

    public final byte batchType;
    public final List<Object> queryOrIdList;
    public final List<List<ByteBuffer>> values;
    public final QueryOptions options;

    public BatchMessage(byte type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, QueryOptions options)
    {
        super(Message.Type.BATCH);
        if (type < 0 && type > 2) {
            throw new ProtocolException("Invalid BATCH message type " + type);
        }
        this.batchType = type;
        this.queryOrIdList = queryOrIdList;
        this.values = values;
        this.options = options;
    }

    @Override
    protected CompletableFuture<? extends Response> execute(Persistence persistence, QueryState state, long queryStartNanoTime)
    {
        CompletableFuture<? extends Result> future = persistence.batch(BatchType.fromId(batchType), queryOrIdList, values, state, options, getCustomPayload(), isTracingRequested(), queryStartNanoTime);
        return future.thenApply(result -> new ResultMessage(result));
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BATCH of [");
        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(queryOrIdList.get(i)).append(" with ").append(values.get(i).size()).append(" values");
        }
        sb.append("] at consistency ").append(options.getConsistency());
        return sb.toString();
    }
}
