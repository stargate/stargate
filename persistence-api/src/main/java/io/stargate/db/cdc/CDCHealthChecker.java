package io.stargate.db.cdc;

import org.apache.cassandra.stargate.db.MutationEvent;

interface CDCHealthChecker extends AutoCloseable {
  void init(int minSamples, double maxFailureRatio, int reSampleAfterMs);

  boolean isHealthy();

  void reportSendError(MutationEvent mutation, Exception ex);

  void reportSendSuccess();
}
