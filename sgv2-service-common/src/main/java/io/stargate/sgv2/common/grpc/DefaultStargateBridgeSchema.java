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
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.GetSchemaNotificationsParams;
import io.stargate.proto.Schema.SchemaNotification;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeStub;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultStargateBridgeSchema implements StargateBridgeSchema {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStargateBridgeSchema.class);

  private static final int KEYSPACE_CACHE_SIZE = 1000;
  private static final Duration KEYSPACE_CACHE_TTL = Duration.ofMinutes(5);
  private static final int DESCRIBE_KEYSPACE_TIMEOUT_SECONDS = 5;
  private static final GetSchemaNotificationsParams GET_SCHEMA_NOTIFICATIONS_PARAMS =
      GetSchemaNotificationsParams.newBuilder().build();
  private static final long MAX_RECONNECT_DELAY_MS = TimeUnit.SECONDS.toMillis(5);

  private final Channel channel;
  private final StargateBearerToken adminCallCredentials;
  // Each entry represents the result of the query (that may be complete or still in progress) to
  // describe a given keyspace. No entry means we know nothing about the keyspace, the first client
  // to ask for it will trigger the query. `CompletionStage<null>` means the keyspace does not
  // exist. Schema change notifications invalidate the entry to force a refresh.
  private final Cache<String, CompletionStage<CqlKeyspaceDescribe>> keyspaceCache;
  private final ScheduledExecutorService executor;
  private volatile long reconnectDelayMs = 0;

  DefaultStargateBridgeSchema(
      Channel channel, String adminAuthToken, ScheduledExecutorService executor) {
    this.channel = channel;
    this.adminCallCredentials = new StargateBearerToken(adminAuthToken);
    this.keyspaceCache =
        Caffeine.newBuilder()
            .maximumSize(KEYSPACE_CACHE_SIZE)
            .expireAfterAccess(KEYSPACE_CACHE_TTL)
            .build();
    this.executor = executor;

    registerChangeObserver();
  }

  @Override
  public CompletionStage<CqlKeyspaceDescribe> getKeyspaceAsync(String keyspaceName) {
    CompletionStage<CqlKeyspaceDescribe> existing = keyspaceCache.getIfPresent(keyspaceName);
    if (existing != null) {
      LOG.debug("getKeyspaceAsync({}): Found entry in the cache, returning", keyspaceName);
      return existing;
    }
    // Prepare to query from the gRPC backend ourselves
    CompletableFuture<CqlKeyspaceDescribe> mine = new CompletableFuture<>();
    existing = keyspaceCache.asMap().putIfAbsent(keyspaceName, mine);
    // Check that nobody beat us to it concurrently
    if (existing != null) {
      LOG.debug(
          "getKeyspaceAsync({}): Someone else started loading the entry first, returning",
          keyspaceName);
      return existing;
    }
    executeGrpcQuery(keyspaceName, mine);
    return mine.whenComplete(
        (__, error) -> {
          // Don't leave a failed entry in the cache
          if (error != null) {
            LOG.debug("Error while loading keyspace {} ({})", keyspaceName, error.getMessage());
            removeKeyspace(keyspaceName);
          }
        });
  }

  private void executeGrpcQuery(
      String keyspaceName, CompletableFuture<CqlKeyspaceDescribe> result) {
    LOG.debug("executeGrpcQuery({})", keyspaceName);
    StargateBridgeStub stub =
        newStub().withDeadlineAfter(DESCRIBE_KEYSPACE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    stub.describeKeyspace(
        DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build(),
        new DescribeKeyspaceObserver(keyspaceName, result));
  }

  // Deliberately package-private, we don't want to expose this on the public API
  void removeKeyspace(String keyspaceName) {
    LOG.debug("removeKeyspace({})", keyspaceName);
    keyspaceCache.invalidate(keyspaceName);
  }

  private void registerChangeObserver() {
    LOG.debug("Registering change observer");
    StargateBridgeStub stub = newStub(); // note: no deadline
    stub.getSchemaNotifications(GET_SCHEMA_NOTIFICATIONS_PARAMS, new ChangeObserver());
  }

  private StargateBridgeStub newStub() {
    return StargateBridgeGrpc.newStub(channel).withCallCredentials(adminCallCredentials);
  }

  static class DescribeKeyspaceObserver implements StreamObserver<CqlKeyspaceDescribe> {

    private final String keyspaceName;
    private final CompletableFuture<CqlKeyspaceDescribe> result;

    DescribeKeyspaceObserver(String keyspaceName, CompletableFuture<CqlKeyspaceDescribe> result) {
      this.keyspaceName = keyspaceName;
      this.result = result;
    }

    @Override
    public void onNext(CqlKeyspaceDescribe keyspace) {
      LOG.debug("Loaded entry for {}", keyspaceName);
      result.complete(keyspace);
    }

    @Override
    public void onError(Throwable t) {
      if (t instanceof StatusRuntimeException
          && ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.NOT_FOUND) {
        LOG.debug("Loaded (empty) entry for {}", keyspaceName);
        result.complete(null);
      } else {
        result.completeExceptionally(t);
      }
    }

    @Override
    public void onCompleted() {
      // intentionally empty
    }
  }

  class ChangeObserver implements StreamObserver<SchemaNotification> {
    @Override
    public void onNext(SchemaNotification notification) {
      if (notification.hasReady()) {
        // In case we missed notifications while the stream was initializing:
        keyspaceCache.invalidateAll();
        // Reset for next time we get disconnected:
        reconnectDelayMs = 0;
      } else {
        SchemaChange change = notification.getChange();
        LOG.debug(
            "Received notification {} {} {}",
            change.getChangeType(),
            change.getTarget(),
            change.getKeyspace());
        // Simply invalidate every time, the next client that needs it will reload it
        removeKeyspace(change.getKeyspace());
      }
    }

    @Override
    public void onError(Throwable t) {
      if (t instanceof StatusRuntimeException) {
        LOG.warn("Unexpected gRPC error in notification stream: {}", t.getMessage());
      } else {
        LOG.warn("Unexpected error in notification stream", t);
      }
      scheduleReconnect();
    }

    @Override
    public void onCompleted() {
      // This should never happen since the server implementation never completes.
      // Handle gracefully nonetheless.
      LOG.warn("Unexpected onCompleted");
      scheduleReconnect();
    }
  }

  private void scheduleReconnect() {
    executor.schedule(this::registerChangeObserver, getAndIncreaseDelayMs(), TimeUnit.MILLISECONDS);
  }

  private long getAndIncreaseDelayMs() {
    long current = reconnectDelayMs;
    if (current == 0) {
      reconnectDelayMs = 100;
    } else if (current < MAX_RECONNECT_DELAY_MS) {
      reconnectDelayMs = Math.min(current * 2, MAX_RECONNECT_DELAY_MS);
    }
    LOG.debug("Scheduling reconnection in {} ms", current);
    return current;
  }
}
