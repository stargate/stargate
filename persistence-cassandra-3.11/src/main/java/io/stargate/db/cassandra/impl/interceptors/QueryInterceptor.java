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
package io.stargate.db.cassandra.impl.interceptors;

import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

/**
 * An interface for intercepting queries and node lifecycle events. It's used to intercept
 * `system.local` and `system.peers` queries and topology events for stargate nodes.
 */
public interface QueryInterceptor {
  void initialize();

  Result interceptQuery(
      QueryHandler handler,
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime);

  void register(IEndpointLifecycleSubscriber subscriber);
}
