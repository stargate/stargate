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

package io.stargate.sgv2.docsapi.api.common.properties.datastore.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.impl.DataStorePropertiesImpl;
import io.stargate.sgv2.docsapi.config.DataStoreConfig;
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
      DataStoreConfig dataStoreConfig) {
    DataStoreProperties fromConfig =
        new DataStorePropertiesImpl(
            dataStoreConfig.secondaryIndexesEnabled(),
            dataStoreConfig.saiEnabled(),
            dataStoreConfig.loggedBatchesEnabled());

    // if we should not read from the bridge, go for defaults
    if (dataStoreConfig.ignoreBridge()) {
      return fromConfig;
    }

    try {
      // fire request
      Schema.SupportedFeaturesRequest request =
          Schema.SupportedFeaturesRequest.newBuilder().build();
      Schema.SupportedFeaturesResponse supportedFeatures = bridge.getSupportedFeatures(request);

      // construct props from bridge
      return new DataStorePropertiesImpl(
          supportedFeatures.getSecondaryIndexes(),
          supportedFeatures.getSai(),
          supportedFeatures.getLoggedBatches());
    } catch (Exception e) {
      LOG.warn(
          "Error fetching the data store properties from the bridge, fallback to the configuration based properties.",
          e);
      return fromConfig;
    }
  }
}
