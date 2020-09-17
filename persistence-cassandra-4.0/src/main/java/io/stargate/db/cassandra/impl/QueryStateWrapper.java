package io.stargate.db.cassandra.impl;

import io.stargate.db.ClientState;
import io.stargate.db.QueryState;
import java.net.InetAddress;

public class QueryStateWrapper implements QueryState<org.apache.cassandra.service.QueryState> {
  private org.apache.cassandra.service.QueryState wrapped;
  private ClientState<org.apache.cassandra.service.ClientState> clientStateWrapper;

  QueryStateWrapper(ClientState clientState) {
    clientStateWrapper = clientState;
    wrapped = new org.apache.cassandra.service.QueryState(clientStateWrapper.getWrapped());
  }

  @Override
  public ClientState getClientState() {
    return clientStateWrapper;
  }

  @Override
  public InetAddress getClientAddress() {
    return wrapped.getClientAddress();
  }

  @Override
  public org.apache.cassandra.service.QueryState getWrapped() {
    return wrapped;
  }
}
