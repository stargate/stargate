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
package io.stargate.sgv2.common.grpc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Channel;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.SchemaRead;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultStargateBridgeClientFactory implements StargateBridgeClientFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultStargateBridgeClientFactory.class);

  private final Channel channel;
  private final SchemaRead.SourceApi sourceApi;
  private final List<KeyspaceInvalidationListener> listeners = new CopyOnWriteArrayList<>();
  @VisibleForTesting final Cache<String, CqlKeyspaceDescribe> keyspaceCache;

  DefaultStargateBridgeClientFactory(Channel channel, SchemaRead.SourceApi sourceApi) {
    this(channel, sourceApi, 1000);
  }

  @VisibleForTesting
  DefaultStargateBridgeClientFactory(
      Channel channel, SchemaRead.SourceApi sourceApi, int cacheSize) {
    this.channel = channel;
    this.sourceApi = sourceApi;
    keyspaceCache =
        Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .removalListener(
                (RemovalListener<String, CqlKeyspaceDescribe>)
                    (key, value, cause) -> notifyListeners(key))
            .build();
  }

  @Override
  public StargateBridgeClient newClient(String authToken, Optional<String> tenantId) {
    return new DefaultStargateBridgeClient(channel, authToken, tenantId, keyspaceCache, sourceApi);
  }

  @Override
  public void register(KeyspaceInvalidationListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean unregister(KeyspaceInvalidationListener listener) {
    return listeners.remove(listener);
  }

  private void notifyListeners(String globalKeyspaceName) {
    for (KeyspaceInvalidationListener listener : listeners) {
      try {
        listener.onKeyspaceInvalidated(globalKeyspaceName);
      } catch (Throwable t) {
        LOG.warn("Unexpected error while notifying listener", t);
      }
    }
  }
}
