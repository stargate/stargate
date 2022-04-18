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

package io.stargate.sgv2.docsapi.api.common.properties.configuration;

import io.quarkus.grpc.GrpcClient;
import io.stargate.proto.MutinyStargateBridgeGrpc;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.api.common.properties.model.CombinedProperties;
import io.stargate.sgv2.docsapi.api.common.properties.model.DataStoreProperties;
import io.stargate.sgv2.docsapi.config.DataStoreConfig;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import java.time.Duration;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CombinedPropertiesConfiguration {

  /** Logger for the class. */
  private static final Logger LOG = LoggerFactory.getLogger(CombinedPropertiesConfiguration.class);

  /** The bridge client. */
  private final MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridge;

  /** Document config based on YAML props. */
  private final DocumentConfig documentConfig;

  /** Data store config based on YAML props. */
  private final DataStoreConfig dataStoreConfig;

  @Inject
  public CombinedPropertiesConfiguration(
      @GrpcClient("bridge") MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridge,
      DocumentConfig documentConfig,
      DataStoreConfig dataStoreConfig) {
    this.bridge = bridge;
    this.documentConfig = documentConfig;
    this.dataStoreConfig = dataStoreConfig;
  }

  @Produces
  @ApplicationScoped
  CombinedProperties configuration() {
    CombinedProperties fromConfig = new CombinedProperties(documentConfig, dataStoreConfig);

    // if we should not read from the bridge, go for defaults
    if (!dataStoreConfig.readFromBridge()) {
      return fromConfig;
    }

    try {
      // fire request
      Schema.SupportedFeaturesRequest request =
          Schema.SupportedFeaturesRequest.newBuilder().build();
      Schema.SupportedFeaturesResponse supportedFeatures =
          bridge.getSupportedFeatures(request).await().atMost(Duration.ofSeconds(5));

      // construct props from bridge
      DataStoreProperties propertiesFromBridge =
          new DataStoreProperties() {
            @Override
            public boolean secondaryIndexesEnabled() {
              return supportedFeatures.getSecondaryIndexes();
            }

            @Override
            public boolean saiEnabled() {
              return supportedFeatures.getSai();
            }

            @Override
            public boolean loggedBatchesEnabled() {
              return supportedFeatures.getLoggedBatches();
            }
          };

      return new CombinedProperties(documentConfig, propertiesFromBridge);
    } catch (Exception e) {
      LOG.error(
          "Error fetching the data store properties from the bridge, fallback to the configuration based properties.",
          e);
      return fromConfig;
    }
  }
}
