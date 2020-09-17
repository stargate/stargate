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
