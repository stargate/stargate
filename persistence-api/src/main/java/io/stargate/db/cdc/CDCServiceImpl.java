package io.stargate.db.cdc;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;

public final class CDCServiceImpl implements CDCService {

  private final CDCProducer producer;
  private final CDCHealthChecker healthChecker;
  private final CDCConfig config;
  private static final CompletableFuture<Void> completedFuture =
      CompletableFuture.completedFuture(null);
  private static final CompletableFuture<Void> unhealthyFuture = new CompletableFuture<>();

  static {
    unhealthyFuture.completeExceptionally(
        new CDCWriteException("CDC producer marked as unhealthy"));
  }

  @VisibleForTesting
  public CDCServiceImpl(CDCProducer producer, CDCHealthChecker healthChecker, CDCConfig config) {
    this.producer = producer;
    this.healthChecker = healthChecker;
    this.config = config;
  }

  @Override
  public CompletableFuture<Void> publish(MutationEvent mutation) throws CDCWriteException {
    if (!config.isTrackedByCDC(mutation)) {
      return completedFuture;
    }

    if (!healthChecker.isHealthy()) {
      return unhealthyFuture;
    }

    throw new RuntimeException("Not implemented");
  }

  @Override
  public void close() throws Exception {
    healthChecker.close();
    producer.close();
  }

  @VisibleForTesting
  interface CDCConfig {
    boolean isTrackedByCDC(MutationEvent mutation);
  }
}
