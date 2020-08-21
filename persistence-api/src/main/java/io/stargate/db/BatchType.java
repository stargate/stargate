package io.stargate.db;

import org.apache.cassandra.stargate.transport.ProtocolException;

public enum BatchType
{
    LOGGED(0),
    UNLOGGED(1),
    COUNTER(2);

    public final int id;

    BatchType(int id)
    {
        this.id = id;
    }

    public static BatchType fromId(int id)
    {
        for (BatchType t : values())
        {
            if (t.id == id) return t;
        }
        throw new ProtocolException(String.format("Unknown batch type %d", id));
    }
}
