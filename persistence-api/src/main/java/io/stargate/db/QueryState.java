package io.stargate.db;

import java.net.InetAddress;

public interface QueryState<T>
{
    ClientState<?> getClientState();

    InetAddress getClientAddress();

    T getWrapped();
}
