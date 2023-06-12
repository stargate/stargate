//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.cassandra.stargate.transport.internal;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.AbstractMessageHandler.WaitQueue;
import org.apache.cassandra.net.ResourceLimits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientResourceLimits {
  private static final Logger logger = LoggerFactory.getLogger(ClientResourceLimits.class);
  private static final ResourceLimits.Concurrent GLOBAL_LIMIT =
      new ResourceLimits.Concurrent(getGlobalLimit());
  private static final AbstractMessageHandler.WaitQueue GLOBAL_QUEUE;
  private static final ConcurrentMap<InetAddress, Allocator> PER_ENDPOINT_ALLOCATORS;

  public ClientResourceLimits() {}

  public static Allocator getAllocatorForEndpoint(InetAddress endpoint) {
    while (true) {
      Allocator result =
          (Allocator)
              PER_ENDPOINT_ALLOCATORS.computeIfAbsent(
                  endpoint,
                  (x$0) -> {
                    return new Allocator(x$0);
                  });
      if (result.acquire()) {
        return result;
      }

      PER_ENDPOINT_ALLOCATORS.remove(endpoint, result);
    }
  }

  public static long getGlobalLimit() {
    return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes();
  }

  public static void setGlobalLimit(long newLimit) {
    // Cassandra {4.0.10} Change to use TransportDescriptor
    TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytes(newLimit);
    long existingLimit = GLOBAL_LIMIT.setLimit(getGlobalLimit());
    logger.info(
        "Changed native_max_transport_requests_in_bytes from {} to {}", existingLimit, newLimit);
  }

  public static long getCurrentGlobalUsage() {
    return GLOBAL_LIMIT.using();
  }

  public static long getEndpointLimit() {
    return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
  }

  public static void setEndpointLimit(long newLimit) {
    long existingLimit = TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
    TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytesPerIp(
        newLimit); // ensure new instances get the new limit
    Allocator allocator;
    for (Iterator var4 = PER_ENDPOINT_ALLOCATORS.values().iterator();
        var4.hasNext();
        existingLimit = allocator.endpointAndGlobal.endpoint().setLimit(newLimit)) {
      allocator = (Allocator) var4.next();
    }

    logger.info(
        "Changed native_max_transport_requests_in_bytes_per_ip from {} to {}",
        existingLimit,
        newLimit);
  }

  public static Snapshot getCurrentIpUsage() {
    DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir();
    Iterator var1 = PER_ENDPOINT_ALLOCATORS.values().iterator();

    while (var1.hasNext()) {
      Allocator allocator = (Allocator) var1.next();
      histogram.update(allocator.endpointAndGlobal.endpoint().using());
    }

    return histogram.getSnapshot();
  }

  public static Reservoir ipUsageReservoir() {
    return new Reservoir() {
      public int size() {
        return ClientResourceLimits.PER_ENDPOINT_ALLOCATORS.size();
      }

      public void update(long l) {
        throw new IllegalStateException();
      }

      public Snapshot getSnapshot() {
        return ClientResourceLimits.getCurrentIpUsage();
      }
    };
  }

  static {
    GLOBAL_QUEUE = WaitQueue.global(GLOBAL_LIMIT);
    PER_ENDPOINT_ALLOCATORS = new ConcurrentHashMap();
  }

  interface ResourceProvider {
    ResourceLimits.Limit globalLimit();

    AbstractMessageHandler.WaitQueue globalWaitQueue();

    ResourceLimits.Limit endpointLimit();

    AbstractMessageHandler.WaitQueue endpointWaitQueue();

    void release();

    public static class Default implements ResourceProvider {
      private final Allocator limits;

      Default(Allocator limits) {
        this.limits = limits;
      }

      public ResourceLimits.Limit globalLimit() {
        return this.limits.endpointAndGlobal.global();
      }

      public AbstractMessageHandler.WaitQueue globalWaitQueue() {
        return ClientResourceLimits.GLOBAL_QUEUE;
      }

      public ResourceLimits.Limit endpointLimit() {
        return this.limits.endpointAndGlobal.endpoint();
      }

      public AbstractMessageHandler.WaitQueue endpointWaitQueue() {
        return this.limits.waitQueue;
      }

      public void release() {
        this.limits.release();
      }
    }
  }

  static class Allocator {
    private final AtomicInteger refCount;
    private final InetAddress endpoint;
    private final ResourceLimits.EndpointAndGlobal endpointAndGlobal;
    private final AbstractMessageHandler.WaitQueue waitQueue;

    private Allocator(InetAddress endpoint) {
      this.refCount = new AtomicInteger(0);
      this.endpoint = endpoint;
      ResourceLimits.Concurrent limit =
          new ResourceLimits.Concurrent(ClientResourceLimits.getEndpointLimit());
      this.endpointAndGlobal =
          new ResourceLimits.EndpointAndGlobal(limit, ClientResourceLimits.GLOBAL_LIMIT);
      this.waitQueue = WaitQueue.endpoint(limit);
    }

    private boolean acquire() {
      return 0
          < this.refCount.updateAndGet(
              (i) -> {
                return i < 0 ? i : i + 1;
              });
    }

    void release() {
      if (-1
          == this.refCount.updateAndGet(
              (i) -> {
                return i == 1 ? -1 : i - 1;
              })) {
        ClientResourceLimits.PER_ENDPOINT_ALLOCATORS.remove(this.endpoint, this);
      }
    }

    ResourceLimits.Outcome tryAllocate(long amount) {
      return this.endpointAndGlobal.tryAllocate(amount);
    }

    void allocate(long amount) {
      this.endpointAndGlobal.allocate(amount);
    }

    ResourceLimits.Outcome release(long amount) {
      return this.endpointAndGlobal.release(amount);
    }

    @VisibleForTesting
    long endpointUsing() {
      return this.endpointAndGlobal.endpoint().using();
    }

    @VisibleForTesting
    long globallyUsing() {
      return this.endpointAndGlobal.global().using();
    }

    public String toString() {
      return String.format(
          "InflightEndpointRequestPayload: %d/%d, InflightOverallRequestPayload: %d/%d",
          this.endpointAndGlobal.endpoint().using(),
          this.endpointAndGlobal.endpoint().limit(),
          this.endpointAndGlobal.global().using(),
          this.endpointAndGlobal.global().limit());
    }
  }
}
