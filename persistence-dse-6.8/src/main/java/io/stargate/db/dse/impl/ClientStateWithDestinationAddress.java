package io.stargate.db.dse.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

/**
 * Extends the DSE ClientState class to add an optional 'destination address', so this can be passed
 * down to {@link io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor}.
 */
public class ClientStateWithDestinationAddress extends ClientState {

  private final InetSocketAddress destinationAddress;

  public ClientStateWithDestinationAddress(
      SocketAddress remoteAddress, InetSocketAddress destinationAddress) {
    super((InetSocketAddress) remoteAddress, null);
    this.destinationAddress = destinationAddress;
  }

  public ClientStateWithDestinationAddress(
      AuthenticatedUser user,
      InetSocketAddress remoteAddress,
      InetSocketAddress destinationAddress) {
    // Note: we call the "copy" ctor because it's a visible one, while the one taking the user and
    // the remote address is not.
    super(ClientState.forExternalCalls(user, remoteAddress));
    this.destinationAddress = destinationAddress;
  }

  /**
   * The destination address that the client connected to (if it was provided in a PROXY header).
   */
  public InetSocketAddress destinationAddress() {
    return destinationAddress;
  }
}
