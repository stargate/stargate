package io.stargate.db.dse.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

/**
 * Extends the DSE ClientState class to add an optional 'public address', so this can be passed down
 * to {@link io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor}.
 */
public class ClientStateWithPublicAddress extends ClientState {

  private final InetSocketAddress publicAddress;

  public ClientStateWithPublicAddress(
      SocketAddress remoteAddress, InetSocketAddress publicAddress) {
    super((InetSocketAddress) remoteAddress, null);
    this.publicAddress = publicAddress;
  }

  public ClientStateWithPublicAddress(
      AuthenticatedUser user, InetSocketAddress remoteAddress, InetSocketAddress publicAddress) {
    // Note: we call the "copy" ctor because it's a visible one, while the one taking the user and
    // the remote address is not.
    super(ClientState.forExternalCalls(user, remoteAddress));
    this.publicAddress = publicAddress;
  }

  public InetSocketAddress publicAddress() {
    return publicAddress;
  }
}
