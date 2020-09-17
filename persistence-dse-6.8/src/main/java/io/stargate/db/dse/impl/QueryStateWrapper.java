package io.stargate.db.dse.impl;

import io.stargate.db.ClientState;
import io.stargate.db.QueryState;
import java.net.InetAddress;
import org.apache.cassandra.config.DatabaseDescriptor;

public class QueryStateWrapper implements QueryState<org.apache.cassandra.service.QueryState> {
  private org.apache.cassandra.service.QueryState wrapped;
  private ClientState<org.apache.cassandra.service.ClientState> clientStateWrapper;

  public QueryStateWrapper(ClientState<org.apache.cassandra.service.ClientState> clientState) {
    clientStateWrapper = clientState;

    if (clientStateWrapper.getUser() == null) {
      wrapped =
          new org.apache.cassandra.service.QueryState(
              clientStateWrapper.getWrapped(),
              org.apache.cassandra.auth.user.UserRolesAndPermissions.ANONYMOUS);
    } else {
      wrapped =
          new org.apache.cassandra.service.QueryState(
              clientStateWrapper.getWrapped(),
              DatabaseDescriptor.getAuthManager()
                  .getUserRolesAndPermissions(
                      clientStateWrapper.getUser().getName(),
                      clientStateWrapper.getUser().getName())
                  .blockingGet());
    }
  }

  @Override
  public io.stargate.db.ClientState<org.apache.cassandra.service.ClientState> getClientState() {
    return clientStateWrapper;
  }

  @Override
  public InetAddress getClientAddress() {
    return wrapped.getClientAddress();
  }

  public org.apache.cassandra.service.QueryState getWrapped() {
    return wrapped;
  }
}
