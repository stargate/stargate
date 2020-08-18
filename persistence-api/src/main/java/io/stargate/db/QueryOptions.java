package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public interface QueryOptions<T>
{
    ConsistencyLevel getConsistency();

    List<ByteBuffer> getValues();

    List<String> getNames();

    ProtocolVersion getProtocolVersion();

    int getPageSize();

    ByteBuffer getPagingState();

    ConsistencyLevel getSerialConsistency();

    long getTimestamp();

    int getNowInSeconds();

    String getKeyspace();

    boolean skipMetadata();
}
