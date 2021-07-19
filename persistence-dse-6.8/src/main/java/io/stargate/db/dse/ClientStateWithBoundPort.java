package io.stargate.db.dse;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.cassandra.service.ClientState;

public class ClientStateWithBoundPort extends ClientState {
  private final int boundPort;

  public ClientStateWithBoundPort(SocketAddress remoteAddress, int boundPort) {
    super((InetSocketAddress) remoteAddress, null);
    this.boundPort = boundPort;
  }

  public int boundPort() {
    return boundPort;
  }
}
