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
package io.stargate.sgv2.common.schema;

import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.SchemaChange.Target;
import io.stargate.proto.QueryOuterClass.SchemaChange.Type;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.SchemaNotification;
import io.stargate.proto.StargateGrpc.StargateStub;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.InitReplayingStreamObserver;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaCache {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaCache.class);

  public static SchemaCache newInstance(StargateStub stub) {
    SchemaCache cache = new SchemaCache(stub);
    // Blocking is fine here, the caller will be some application initializer (e.g. DropWizard's
    // Application.run())
    Futures.getUninterruptibly(cache.refreshKeyspaces(true));
    return cache;
  }

  private final StargateStub stub;
  private final ConcurrentMap<String, CqlKeyspaceDescribe> keyspaces = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<SchemaListener> listeners = new CopyOnWriteArrayList<>();

  private volatile Observer observer;

  private SchemaCache(StargateStub stub) {
    this.stub = stub;
  }

  public CqlKeyspaceDescribe getKeyspace(String keyspaceName) {
    CqlKeyspaceDescribe keyspace = keyspaces.get(keyspaceName);
    if (keyspace == null) {
      throw new IllegalArgumentException("Unknown keyspace " + keyspaceName);
    } else {
      return keyspace;
    }
  }

  public void register(SchemaListener listener) {
    listeners.add(listener);
  }

  public void unregister(SchemaListener listener) {
    listeners.remove(listener);
  }

  /** Get a fresh copy of the keyspaces, and (re)install the observer. */
  private CompletionStage<Void> refreshKeyspaces(boolean isInit) {
    // Register the observer early, in case the schema changes while we are fetching the initial
    // version.
    // We don't need to cancel the previous observer: we get here either because it
    // failed/completed, or at init time where it's null.
    observer = new Observer();
    SchemaCacheGrpc.registerChangeObserver(stub, observer);

    return SchemaCacheGrpc.getAllKeyspaces(stub)
        .thenApply(
            newKeyspaces -> {
              // Added or updated
              for (CqlKeyspaceDescribe keyspace : newKeyspaces.values()) {
                updateKeyspace(keyspace, isInit);
              }

              // Removed
              for (String keyspaceName : new HashSet<>(keyspaces.keySet())) {
                if (!newKeyspaces.containsKey(keyspaceName)) {
                  deleteKeyspace(keyspaceName);
                }
              }

              // If any changes were recorded, they will be replayed now. We don't know which ones
              // we missed, but reapplying them is idempotent, at worst it's a bit of duplicated
              // work.
              observer.markReady();

              return null;
            });
  }

  private void updateKeyspace(CqlKeyspaceDescribe newKeyspace, boolean isInit) {
    String keyspaceName = newKeyspace.getCqlKeyspace().getName();
    CqlKeyspaceDescribe oldKeyspace = keyspaces.put(keyspaceName, newKeyspace);
    if (isInit) {
      assert oldKeyspace == null;
      // don't send notifications
    } else if (oldKeyspace == null) {
      LOG.debug("Added keyspace {}", keyspaceName);
      notifyListeners(l -> l.onCreateKeyspace(newKeyspace));
    } else {
      LOG.debug("Updated keyspace {}", keyspaceName);
      notifyListeners(l -> l.onUpdateKeyspace(oldKeyspace, newKeyspace));
    }
  }

  private void deleteKeyspace(String keyspaceName) {
    CqlKeyspaceDescribe oldKeyspace = keyspaces.remove(keyspaceName);
    if (oldKeyspace != null) {
      LOG.debug("Removed keyspace {}", keyspaceName);
      notifyListeners(l -> l.onDropKeyspace(oldKeyspace));
    }
  }

  private void notifyListeners(Consumer<SchemaListener> action) {
    for (SchemaListener listener : listeners) {
      try {
        action.accept(listener);
      } catch (Throwable t) {
        LOG.error("Unexpected error while invoking " + listener, t);
      }
    }
  }

  private class Observer extends InitReplayingStreamObserver<SchemaNotification> {

    @Override
    protected void onNextReady(SchemaNotification notification) {
      SchemaChange change = notification.getChange();
      if (notification.hasKeyspace()) {
        updateKeyspace(notification.getKeyspace(), false);
      } else {
        assert change.getChangeType() == Type.DROPPED && change.getTarget() == Target.KEYSPACE;
        deleteKeyspace(change.getKeyspace());
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Notification stream failed, trying to reconnect", t);
      tryReconnect();
    }

    @Override
    public void onCompleted() {
      LOG.error("Notification stream completed unexpectedly, trying to reconnect");
      tryReconnect();
    }

    private void tryReconnect() {
      refreshKeyspaces(false)
          .exceptionally(
              t -> {
                LOG.error(
                    "Error while trying to reconnect to notification stream, "
                        + "the schema cache will stop updating",
                    t);
                return null;
              });
    }
  }
}
