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
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.api.common.properties.datastore.impl.DataStorePropertiesImpl;
import java.time.temporal.ChronoUnit;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import org.eclipse.microprofile.faulttolerance.Retry;
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
      DataStoreConfig dataStoreConfig) {
    DataStoreProperties fromConfig =
        new DataStorePropertiesImpl(
            dataStoreConfig.secondaryIndexesEnabled(),
            dataStoreConfig.saiEnabled(),
            dataStoreConfig.loggedBatchesEnabled());

    // if we should not read from the bridge, go for defaults
    if (dataStoreConfig.ignoreBridge()) {
      LOG.info("DataStoreConfig.ignoreBridge() == true, will use pre-configured defaults");
      return fromConfig;
    }

    LOG.info(
        "DataStoreConfig.ignoreBridge() == false, will try to fetch data store metadata using the Bridge");
    try {
      // fire request
      Schema.SupportedFeaturesResponse supportedFeatures = fetchSupportedFeatures(bridge);

      // construct props from bridge
      return new DataStorePropertiesImpl(
          supportedFeatures.getSecondaryIndexes(),
          supportedFeatures.getSai(),
          supportedFeatures.getLoggedBatches());
    } catch (Exception e) {
      if (dataStoreConfig.useFallbacksIfCallFails()) {
        LOG.warn(
            "Failed to fetch the data store metadata, 'useFallbacksIfCallFails' == true, will return fallback data store metadata",
            e);
        return fromConfig;
      }
      LOG.warn(
          "Failed to fetch the data store metadata, 'useFallbacksIfCallFails' == false, will fail");
      throw e;
    }
  }

  /**
   * Method for fetching data store metadata: will attempt up to 5 retries, for up to 1 minute
   * before failing.
   */
  @Retry(
      maxRetries = 5,
      delay = 5,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 60,
      durationUnit = ChronoUnit.SECONDS)
  private Schema.SupportedFeaturesResponse fetchSupportedFeatures(
      StargateBridgeGrpc.StargateBridgeBlockingStub bridge) {
    try {
      return bridge.getSupportedFeatures(Schema.SupportedFeaturesRequest.newBuilder().build());
    } catch (Exception e) {
      LOG.warn("Data store metadata fetch using Bridge failed, problem: {}", e.getMessage());
      throw e;
    }
  }
}
