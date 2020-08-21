package io.stargate.db.cassandra.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientState;

public class ClientStateWrapper implements ClientState<org.apache.cassandra.service.ClientState>
{
    private org.apache.cassandra.service.ClientState wrapped;
    private AuthenticatedUser<?> user;

    private ClientStateWrapper(org.apache.cassandra.service.ClientState wrapped)
    {
        if (wrapped.getUser() != null)
            this.user = new AuthenticatorWrapper.AuthenticatedUserWrapper(wrapped.getUser());
        this.wrapped = wrapped;
    }

    public static ClientStateWrapper forExternalCalls(SocketAddress remoteAddress)
    {
        return new ClientStateWrapper(org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress));
    }

    public static ClientStateWrapper forInternalCalls()
    {
        return new ClientStateWrapper(org.apache.cassandra.service.ClientState.forInternalCalls());
    }

    @Override
    public InetSocketAddress getRemoteAddress()
    {
        return wrapped.getRemoteAddress();
    }

    @Override
    public String getRawKeyspace()
    {
        return wrapped.getRawKeyspace();
    }

    @Override
    public Optional<String> getDriverName()
    {
        return Optional.empty();
    }

    @Override
    public void setDriverName(String name)
    {
    }

    @Override
    public Optional<String> getDriverVersion()
    {
        return Optional.empty();
    }

    @Override
    public void setDriverVersion(String version)
    {
    }

    @Override
    public void login(AuthenticatedUser<?> user)
    {
        wrapped.login((org.apache.cassandra.auth.AuthenticatedUser) user.getWrapped());
        this.user = user;
    }

    @Override
    public AuthenticatedUser<?> getUser()
    {
        return user;
    }

    @Override
    public org.apache.cassandra.service.ClientState getWrapped()
    {
        return wrapped;
    }
}
