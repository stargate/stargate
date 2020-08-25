package io.stargate.db;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

public interface ClientState<T>
{
    InetSocketAddress getRemoteAddress();

    InetSocketAddress getPublicAddress();

    String getRawKeyspace();

    Optional<String> getDriverName();

    void setDriverName(String name);

    Optional<String> getDriverVersion();

    void setDriverVersion(String version);

    void login(AuthenticatedUser<?> user);

    AuthenticatedUser<?> getUser();

    T getWrapped();
}
