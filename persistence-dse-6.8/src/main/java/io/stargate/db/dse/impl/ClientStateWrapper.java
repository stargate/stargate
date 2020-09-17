package io.stargate.db.dse.impl;

import io.stargate.db.AuthenticatedUser;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import org.apache.cassandra.service.ClientState;

public class ClientStateWrapper implements io.stargate.db.ClientState<ClientState> {
  private org.apache.cassandra.service.ClientState wrapped;
  private InetSocketAddress publicAddress;
  private AuthenticatedUser<?> user;

  private ClientStateWrapper(org.apache.cassandra.service.ClientState wrapped) {
    if (wrapped.getUser() != null)
      this.user = new AuthenticatorWrapper.AuthenticatedUserWrapper(wrapped.getUser());
    this.wrapped = wrapped;
  }

  private ClientStateWrapper(
      org.apache.cassandra.service.ClientState wrapped, InetSocketAddress publicAddress) {
    this(wrapped);
    this.publicAddress = publicAddress;
  }

  public static ClientStateWrapper forExternalCalls(SocketAddress remoteAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress, null));
  }

  public static ClientStateWrapper forExternalCalls(
      SocketAddress remoteAddress, InetSocketAddress publicAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress, null),
        publicAddress);
  }

  public static ClientStateWrapper forExternalCalls(
      org.apache.cassandra.auth.AuthenticatedUser user,
      InetSocketAddress remoteAddress,
      InetSocketAddress publicAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(user, remoteAddress),
        publicAddress);
  }

  public static ClientStateWrapper forInternalCalls() {
    return new ClientStateWrapper(org.apache.cassandra.service.ClientState.forInternalCalls());
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    return null;
  }

  @Override
  public InetSocketAddress getPublicAddress() {
    return publicAddress;
  }

  @Override
  public String getRawKeyspace() {
    return null;
  }

  @Override
  public Optional<String> getDriverName() {
    return Optional.empty();
  }

  @Override
  public void setDriverName(String name) {}

  @Override
  public Optional<String> getDriverVersion() {
    return Optional.empty();
  }

  @Override
  public void setDriverVersion(String version) {}

  @Override
  public void login(AuthenticatedUser<?> user) {
    wrapped =
        wrapped
            .login((org.apache.cassandra.auth.AuthenticatedUser) user.getWrapped())
            .blockingGet();
    this.user = user;
  }

  @Override
  public AuthenticatedUser<?> getUser() {
    return user;
  }

  public ClientState getWrapped() {
    return wrapped;
  }
}
