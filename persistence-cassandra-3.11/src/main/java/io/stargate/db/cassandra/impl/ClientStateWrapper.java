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

  private ClientStateWrapper(
      org.apache.cassandra.service.ClientState wrapped, InetSocketAddress publicAddress) {
    this(wrapped);
    this.publicAddress = publicAddress;
  }

  private ClientStateWrapper(org.apache.cassandra.service.ClientState wrapped) {
    if (wrapped.getUser() != null)
      this.user = new AuthenticatorWrapper.AuthenticatedUserWrapper(wrapped.getUser());
    this.wrapped = wrapped;
  }

  public static ClientStateWrapper forExternalCalls(
      SocketAddress remoteAddress, InetSocketAddress publicAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress), publicAddress);
  }

  public static ClientStateWrapper forExternalCalls(SocketAddress remoteAddress) {
    return new ClientStateWrapper(
        org.apache.cassandra.service.ClientState.forExternalCalls(remoteAddress));
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
