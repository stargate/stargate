//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.cassandra.utils.memory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.stargate.transport.internal.TransportDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPools {
  private static final Logger logger = LoggerFactory.getLogger(BufferPools.class);
  private static final long FILE_MEMORY_USAGE_THRESHOLD =
      (long) TransportDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;
  private static final BufferPool CHUNK_CACHE_POOL;
  private static final long NETWORKING_MEMORY_USAGE_THRESHOLD;
  private static final BufferPool NETWORKING_POOL;

  public BufferPools() {}

  public static BufferPool forChunkCache() {
    return CHUNK_CACHE_POOL;
  }

  public static BufferPool forNetworking() {
    return NETWORKING_POOL;
  }

  public static void shutdownLocalCleaner(long timeout, TimeUnit unit)
      throws TimeoutException, InterruptedException {
    CHUNK_CACHE_POOL.shutdownLocalCleaner(timeout, unit);
    NETWORKING_POOL.shutdownLocalCleaner(timeout, unit);
  }

  static {
    CHUNK_CACHE_POOL = new BufferPool("chunk-cache", FILE_MEMORY_USAGE_THRESHOLD, true);
    NETWORKING_MEMORY_USAGE_THRESHOLD =
        (long) TransportDescriptor.getNetworkingCacheSizeInMB() * 1024L * 1024L;
    NETWORKING_POOL = new BufferPool("networking", NETWORKING_MEMORY_USAGE_THRESHOLD, false);
    logger.info(
        "Global buffer pool limit is {} for {} and {} for {}",
        new Object[] {
          FBUtilities.prettyPrintMemory(FILE_MEMORY_USAGE_THRESHOLD),
          CHUNK_CACHE_POOL.name,
          FBUtilities.prettyPrintMemory(NETWORKING_MEMORY_USAGE_THRESHOLD),
          NETWORKING_POOL.name
        });
    CHUNK_CACHE_POOL.metrics().register3xAlias();
  }
}
