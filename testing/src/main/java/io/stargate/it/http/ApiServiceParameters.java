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
package io.stargate.it.http;

import java.util.Collections;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Defines parameters for each of the Service API instances requested by a test.
 *
 * @see ApiServiceSpec#parametersCustomizer()
 */
@Value.Immutable(prehash = true)
public interface ApiServiceParameters {

  @Value.Default
  default Map<String, String> systemProperties() {
    return Collections.emptyMap();
  }

  @Value.Default
  default String listenAddress() {
    // Note: 127.0.1.N addresses are used by proxy protocol testing,
    // 127.0.2.N addresses are used for Stargate coordinator nodes,
    // so we allocate from the 127.0.3.X range here, to avoid conflicts with other
    // services that may be listening on the common range of 127.0.0.Y addresses.
    return "127.0.3.1";
  }

  @Value.Default
  // Example: "REST API"
  default String serviceName() {
    return "Override me";
  }

  @Value.Default
  // Example: 8088
  default int servicePort() {
    return 0;
  }

  @Value.Default
  // Example: dw.server.connector.port
  default String servicePortPropertyName() {
    return "Override me";
  }

  @Value.Default
  // Example: 8088
  default int metricsPort() {
    return 0;
  }

  @Value.Default
  // Example: "Started RestServiceServer"
  default String serviceStartedMessage() {
    return "Override me";
  }

  @Value.Default
  // Example: "stargate.rest.libdir"
  default String serviceLibDirProperty() {
    return "Override me";
  }

  @Value.Default
  // example: "sgv2-rest-service"
  default String serviceJarBase() {
    return "Override me";
  }

  @Value.Default
  // example: "dw.stargate.grpc.host"
  default String bridgeHostPropertyName() {
    return "Override me";
  }

  @Value.Default
  // example: "dw.stargate.grpc.port"
  default String bridgePortPropertyName() {
    return "Override me";
  }

  @Value.Default
  // standard value
  default String bridgeTokenPropertyName() {
    return "stargate.bridge.admin_token";
  }

  static Builder builder() {
    return ImmutableApiServiceParameters.builder();
  }

  interface Builder {

    Builder putSystemProperties(String key, String value);

    Builder serviceName(String name);

    Builder servicePort(int port);

    Builder servicePortPropertyName(String name);

    Builder metricsPort(int port);

    Builder serviceStartedMessage(String message);

    Builder serviceLibDirProperty(String serviceLibDirProperty);

    Builder serviceJarBase(String serviceJarBase);

    Builder bridgeHostPropertyName(String bridgeHostPropertyName);

    Builder bridgePortPropertyName(String bridgePortPropertyName);

    Builder bridgeTokenPropertyName(String bridgeTokenPropertyName);

    ApiServiceParameters build();
  }
}
