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
package org.apache.cassandra.stargate.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBCodec;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.utils.Pair;

import io.stargate.db.QueryOptions;
import io.netty.buffer.ByteBuf;

public class DefaultQueryOptions implements QueryOptions {
    public static final CBCodec<QueryOptions> codec = new Codec();

    private final ConsistencyLevel consistency;
    private final List<ByteBuffer> values;
    private final List<String> names;
    private final boolean skipMetadata;
    private final ProtocolVersion version;
    private final SpecificOptions options;

    public DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, List<String> names, boolean skipMetadata, SpecificOptions options, ProtocolVersion version) {
        this.consistency = consistency;
        this.values = values;
        this.names = names;
        this.skipMetadata = skipMetadata;
        this.options = options;
        this.version = version;
    }

    @Override
    public ConsistencyLevel getConsistency() { return consistency; }

    @Override
    public List<ByteBuffer> getValues() { return values; }

    @Override
    public List<String> getNames() { return names; }

    @Override
    public ProtocolVersion getProtocolVersion() { return version; }

    @Override
    public int getPageSize() { return getSpecificOptions().pageSize; }

    @Override
    public ByteBuffer getPagingState() { return getSpecificOptions().pagingState; }

    @Override
    public ConsistencyLevel getSerialConsistency() { return getSpecificOptions().serialConsistency; }

    @Override
    public long getTimestamp() { return getSpecificOptions().timestamp; }

    @Override
    public int getNowInSeconds() { return getSpecificOptions().nowInSeconds; }

    /** The keyspace that this query is bound to, or null if not relevant. */
    @Override
    public String getKeyspace() { return getSpecificOptions().keyspace; }

    @Override
    public boolean skipMetadata() { return false; }

    private SpecificOptions getSpecificOptions() { return options; }

    static class SpecificOptions
    {
        public static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null, Long.MIN_VALUE, null, Integer.MIN_VALUE);

        private final int pageSize;
        private final ByteBuffer pagingState;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;
        private final String keyspace;
        private final int nowInSeconds;

        public SpecificOptions(int pageSize,
                                ByteBuffer state,
                                ConsistencyLevel serialConsistency,
                                long timestamp,
                                String keyspace,
                                int nowInSeconds)
        {
            this.pageSize = pageSize;
            this.pagingState = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
            this.keyspace = keyspace;
            this.nowInSeconds = nowInSeconds;
        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private enum Flag
        {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY,
            TIMESTAMP,
            NAMES_FOR_VALUES,
            KEYSPACE,
            NOW_IN_SECONDS;

            private static final DefaultQueryOptions.Codec.Flag[] ALL_VALUES = values();

            public static EnumSet<DefaultQueryOptions.Codec.Flag> deserialize(int flags)
            {
                EnumSet<DefaultQueryOptions.Codec.Flag> set = EnumSet.noneOf(DefaultQueryOptions.Codec.Flag.class);
                for (int n = 0; n < ALL_VALUES.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(ALL_VALUES[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<DefaultQueryOptions.Codec.Flag> flags)
            {
                int i = 0;
                for (DefaultQueryOptions.Codec.Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }

        public QueryOptions decode(ByteBuf body, ProtocolVersion version)
        {
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<DefaultQueryOptions.Codec.Flag> flags = DefaultQueryOptions.Codec.Flag.deserialize(version.isGreaterOrEqualTo(ProtocolVersion.V5)
                    ? (int)body.readUnsignedInt()
                    : (int)body.readUnsignedByte());

            List<ByteBuffer> values = Collections.<ByteBuffer>emptyList();
            List<String> names = null;
            if (flags.contains(DefaultQueryOptions.Codec.Flag.VALUES))
            {
                if (flags.contains(DefaultQueryOptions.Codec.Flag.NAMES_FOR_VALUES))
                {
                    Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body, version);
                    names = namesAndValues.left;
                    values = namesAndValues.right;
                }
                else
                {
                    values = CBUtil.readValueList(body, version);
                }
            }

            boolean skipMetadata = flags.contains(DefaultQueryOptions.Codec.Flag.SKIP_METADATA);
            flags.remove(DefaultQueryOptions.Codec.Flag.VALUES);
            flags.remove(DefaultQueryOptions.Codec.Flag.SKIP_METADATA);

            DefaultQueryOptions.SpecificOptions options = DefaultQueryOptions.SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                int pageSize = flags.contains(DefaultQueryOptions.Codec.Flag.PAGE_SIZE) ? body.readInt() : -1;
                ByteBuffer pagingState = flags.contains(DefaultQueryOptions.Codec.Flag.PAGING_STATE) ? CBUtil.readValueNoCopy(body) : null;
                ConsistencyLevel serialConsistency = flags.contains(DefaultQueryOptions.Codec.Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;
                long timestamp = Long.MIN_VALUE;
                if (flags.contains(DefaultQueryOptions.Codec.Flag.TIMESTAMP))
                {
                    long ts = body.readLong();
                    if (ts == Long.MIN_VALUE)
                        throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", Long.MIN_VALUE + 1, Long.MAX_VALUE, ts));
                    timestamp = ts;
                }
                String keyspace = flags.contains(DefaultQueryOptions.Codec.Flag.KEYSPACE) ? CBUtil.readString(body) : null;
                int nowInSeconds = flags.contains(DefaultQueryOptions.Codec.Flag.NOW_IN_SECONDS) ? body.readInt() : Integer.MIN_VALUE;
                options = new DefaultQueryOptions.SpecificOptions(pageSize, pagingState, serialConsistency, timestamp, keyspace, nowInSeconds);
            }

            return new DefaultQueryOptions(consistency, values, names, skipMetadata, options, version);
        }

        public void encode(QueryOptions options, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<DefaultQueryOptions.Codec.Flag> flags = gatherFlags(options, version);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                dest.writeInt(DefaultQueryOptions.Codec.Flag.serialize(flags));
            else
                dest.writeByte((byte) DefaultQueryOptions.Codec.Flag.serialize(flags));

            if (flags.contains(DefaultQueryOptions.Codec.Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(DefaultQueryOptions.Codec.Flag.PAGE_SIZE))
                dest.writeInt(options.getPageSize());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.PAGING_STATE))
                CBUtil.writeValue(options.getPagingState(), dest);
            if (flags.contains(DefaultQueryOptions.Codec.Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
            if (flags.contains(DefaultQueryOptions.Codec.Flag.TIMESTAMP))
                dest.writeLong(options.getTimestamp());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.KEYSPACE))
                CBUtil.writeAsciiString(options.getKeyspace(), dest);
            if (flags.contains(DefaultQueryOptions.Codec.Flag.NOW_IN_SECONDS))
                dest.writeInt(options.getNowInSeconds());

            // Note that we don't really have to bother with NAMES_FOR_VALUES server side,
            // and in fact we never really encode QueryOptions, only decode them, so we
            // don't bother.
        }

        public int encodedSize(QueryOptions options, ProtocolVersion version)
        {
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<DefaultQueryOptions.Codec.Flag> flags = gatherFlags(options, version);
            size += (version.isGreaterOrEqualTo(ProtocolVersion.V5) ? 4 : 1);

            if (flags.contains(DefaultQueryOptions.Codec.Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(DefaultQueryOptions.Codec.Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(options.getPagingState());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.TIMESTAMP))
                size += 8;
            if (flags.contains(DefaultQueryOptions.Codec.Flag.KEYSPACE))
                size += CBUtil.sizeOfAsciiString(options.getKeyspace());
            if (flags.contains(DefaultQueryOptions.Codec.Flag.NOW_IN_SECONDS))
                size += 4;

            return size;
        }

        private EnumSet<DefaultQueryOptions.Codec.Flag> gatherFlags(QueryOptions options, ProtocolVersion version)
        {
            EnumSet<DefaultQueryOptions.Codec.Flag> flags = EnumSet.noneOf(DefaultQueryOptions.Codec.Flag.class);
            if (options.getValues().size() > 0)
                flags.add(DefaultQueryOptions.Codec.Flag.VALUES);
            if (options.skipMetadata())
                flags.add(DefaultQueryOptions.Codec.Flag.SKIP_METADATA);
            if (options.getPageSize() >= 0)
                flags.add(DefaultQueryOptions.Codec.Flag.PAGE_SIZE);
            if (options.getPagingState() != null)
                flags.add(DefaultQueryOptions.Codec.Flag.PAGING_STATE);
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(DefaultQueryOptions.Codec.Flag.SERIAL_CONSISTENCY);
            if (options.getTimestamp() != Long.MIN_VALUE)
                flags.add(DefaultQueryOptions.Codec.Flag.TIMESTAMP);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                if (options.getKeyspace() != null)
                    flags.add(DefaultQueryOptions.Codec.Flag.KEYSPACE);
                if (options.getNowInSeconds() != Integer.MIN_VALUE)
                    flags.add(DefaultQueryOptions.Codec.Flag.NOW_IN_SECONDS);
            }

            return flags;
        }
    }
}
