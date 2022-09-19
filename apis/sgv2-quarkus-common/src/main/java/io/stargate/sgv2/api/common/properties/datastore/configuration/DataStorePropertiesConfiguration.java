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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.properties.datastore.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import io.stargate.sgv2.api.common.config.BridgeBootstrapConfig;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.api.common.config.RetryableCallsConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.api.common.properties.datastore.impl.DataStorePropertiesImpl;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStorePropertiesConfiguration {

  /** Logger for the class. */
  private static final Logger LOG = LoggerFactory.getLogger(DataStorePropertiesConfiguration.class);

  @Produces
  @ApplicationScoped
  @Startup
  DataStoreProperties configuration(
      @GrpcClient("bridge") StargateBridgeGrpc.StargateBridgeBlockingStub bridge,
      DataStoreConfig dataStoreConfig,
      BridgeBootstrapConfig bridgeBootstrapConfig)
      throws Exception {
    // if we should not read from the bridge, go for defaults
    if (dataStoreConfig.ignoreBridge()) {
      return new DataStorePropertiesImpl(
          dataStoreConfig.secondaryIndexesEnabled(),
          dataStoreConfig.saiEnabled(),
          dataStoreConfig.loggedBatchesEnabled());
    }

    // Make first attempt separately since we want to log first failure bit differently
    final RetryableCallsConfig retryConfig = bridgeBootstrapConfig.retry();
    final long startTime = System.currentTimeMillis();
    Exception lastFail = null;

    LOG.info("Making the first call to fetch the data store properties via Bridge");
    try {
      return tryFetchSupportedFeatures(bridge);
    } catch (Exception e) {
      lastFail = e;
      LOG.warn(
          "Failed the first call to fetch the data store properties via Bridge, problem: {}",
          lastFail.getMessage());
    }

    final int maxCalls = retryConfig.maxCalls();
    final long lastCallStart = startTime + retryConfig.maxTime().toMillis();
    long currDelay = retryConfig.initialDelay().toMillis();

    for (int calls = 1; ; ++calls) {
      if (calls >= maxCalls) {
        LOG.error(
            String.format(
                "Maximum number of calls (%d) reached: cannot retry, fail after %d calls. Last failure: %d",
                maxCalls, calls, lastFail.getMessage()),
            lastFail);
        throw lastFail;
      }
      final long currTime = System.currentTimeMillis();
      if (currTime > lastCallStart) {
        LOG.error(
            String.format(
                "Maximum bootstrapping time (%s) reached: cannot retry, fail after %d calls. Last failure: %d",
                retryConfig.maxTime(), calls, lastFail.getMessage()),
            lastFail);
        throw lastFail;
      }
      // We want to wait for a bit: delay is from start-to-start, we deduct call time itself.
      long waitUntil = Math.min(lastCallStart, currTime + currDelay);
      // Will always apply at least 10 milliseconds, however
      final long delayMsecs = Math.max(10L, waitUntil - currTime);
      if (delayMsecs > 0) {
        LOG.info("Waiting for {} msecs before bootstrap call retry #{}", delayMsecs, calls);
        Thread.sleep(delayMsecs);
      }
      currDelay =
          Math.min(
              retryConfig.maxDelay().toMillis(), (long) (currDelay * retryConfig.delayRatio()));
      try {
        return tryFetchSupportedFeatures(bridge);
      } catch (Exception e) {
        lastFail = e;
        LOG.warn(
            "Failed bootstrap call retry #{} to fetch the data store properties via Bridge, problem: {}",
            calls,
            lastFail.getMessage());
      }
    }
  }

  private DataStoreProperties tryFetchSupportedFeatures(
      StargateBridgeGrpc.StargateBridgeBlockingStub bridge) throws Exception {
    // fire request
    Schema.SupportedFeaturesRequest request = Schema.SupportedFeaturesRequest.newBuilder().build();
    Schema.SupportedFeaturesResponse supportedFeatures = bridge.getSupportedFeatures(request);

    // construct props from bridge
    return new DataStorePropertiesImpl(
        supportedFeatures.getSecondaryIndexes(),
        supportedFeatures.getSai(),
        supportedFeatures.getLoggedBatches());
  }
}
