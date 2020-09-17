package io.stargate.db.cassandra.impl;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientState;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

public class ClientStateWrapper implements ClientState<org.apache.cassandra.service.ClientState> {
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
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress));
  }

  public static ClientStateWrapper forExternalCalls(
      SocketAddress remoteAddress, InetSocketAddress publicAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress), publicAddress);
  }

  public static ClientStateWrapper forInternalCalls() {
    return new ClientStateWrapper(org.apache.cassandra.service.ClientState.forInternalCalls());
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    return wrapped.getRemoteAddress();
  }

  @Override
  public InetSocketAddress getPublicAddress() {
    return publicAddress;
  }

  @Override
  public String getRawKeyspace() {
    return wrapped.getRawKeyspace();
  }

  @Override
  public Optional<String> getDriverName() {
    return wrapped.getDriverName();
  }

  @Override
  public void setDriverName(String name) {
    wrapped.setDriverName(name);
  }

  @Override
  public Optional<String> getDriverVersion() {
    return wrapped.getDriverVersion();
  }

  @Override
  public void setDriverVersion(String version) {
    wrapped.setDriverVersion(version);
  }

  @Override
  public void login(AuthenticatedUser<?> user) {
    wrapped.login((org.apache.cassandra.auth.AuthenticatedUser) user.getWrapped());
    this.user = user;
  }

  @Override
  public AuthenticatedUser<?> getUser() {
    return user;
  }

  @Override
  public org.apache.cassandra.service.ClientState getWrapped() {
    return wrapped;
  }
}
