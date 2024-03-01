package io.stargate.db.dse.impl;

import io.stargate.db.ClientInfo;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

/** Extends the DSE client state to add information relevant to the Stargate internals. */
public class StargateClientState extends ClientState {

  private final Optional<InetSocketAddress> proxyDestinationAddress;
  private final Optional<Integer> boundPort;
  private volatile boolean isExternalAuth;

  public static StargateClientState forExternalCalls(ClientInfo clientInfo) {
    return clientInfo.destinationAddress().isPresent()
        ? new StargateClientState(
            clientInfo.remoteAddress(), clientInfo.destinationAddress(), Optional.empty())
        : new StargateClientState(
            clientInfo.remoteAddress(), Optional.empty(), Optional.of(clientInfo.boundPort()));
  }

  public static StargateClientState forExternalCalls(AuthenticatedUser user) {
    return new StargateClientState(
        ClientState.forExternalCalls(user), Optional.empty(), Optional.empty(), false);
  }

  public static StargateClientState forInternalCalls() {
    return new StargateClientState(
        ClientState.forInternalCalls(), Optional.empty(), Optional.empty(), false);
  }

  private StargateClientState(
      SocketAddress remoteAddress,
      Optional<InetSocketAddress> proxyDestinationAddress,
      Optional<Integer> boundPort) {
    super((InetSocketAddress) remoteAddress, null);
    this.proxyDestinationAddress = proxyDestinationAddress;
    this.boundPort = boundPort;
  }

  private StargateClientState(
      ClientState source,
      Optional<InetSocketAddress> proxyDestinationAddress,
      Optional<Integer> boundPort,
      boolean isExternalAuth) {
    super(source);
    this.proxyDestinationAddress = proxyDestinationAddress;
    this.boundPort = boundPort;
    this.isExternalAuth = isExternalAuth;
  }

  /**
   * The destination address that the client connected to, if it was provided in a PROXY header.
   *
   * <p>Note that either this or {@link #boundPort()} can be present, but not both.
   */
  public Optional<InetSocketAddress> proxyDestinationAddress() {
    return proxyDestinationAddress;
  }

  /**
   * The port that the TCP socket was established to, if no PROXY header was provided.
   *
   * <p>Note that either this or {@link #proxyDestinationAddress()} can be present, but not both.
   */
  public Optional<Integer> boundPort() {
    return boundPort;
  }

  void setExternalAuth(boolean externalAuth) {
    this.isExternalAuth = externalAuth;
  }

  /**
   * Whether this client was externally authenticated, thereby giving them higher access in terms of
   * Cassandra roles since we trust the checks happened already.
   */
  boolean isExternalAuth() {
    return isExternalAuth;
  }

  @Override
  public ClientState cloneWithKeyspaceIfSet(String keyspace) {
    if (keyspace == null || keyspace.equals(getRawKeyspace())) {
      return this;
    }
    StargateClientState clone =
        new StargateClientState(this, proxyDestinationAddress, boundPort, isExternalAuth);
    clone.setKeyspace(keyspace);
    return clone;
  }
}
