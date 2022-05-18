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
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Defines parameters for each of the Service API instances requested by a test. Recommended usage:
 * provide a base class for tests for each Service API that provides a method to customize these
 * parameters as needed.
 *
 * @see ApiServiceSpec#parametersCustomizer()
 * @see BaseRestApiTest#buildApiServiceParameters(Builder)
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

  @Value.Parameter
  // Example: "REST API"
  String serviceName();

  @Value.Parameter
  // Example: 8082
  int servicePort();

  @Value.Parameter
  String servicePortPropertyName();

  @Value.Parameter
  // Example: 8084
  int metricsPort();

  @Value.Parameter
  // Example: "Started RestServiceServer"
  String serviceStartedMessage();

  @Value.Parameter
  // Example: "stargate.rest.libdir"
  String serviceLibDirProperty();

  @Value.Parameter
  // example: "sgv2-rest-service"
  String serviceJarBase();

  /** The arguments to pass to the service starter class. */
  @Value.Parameter
  List<String> serviceArguments();

  @Value.Parameter
  // example: "dw.stargate.bridge.host"
  String bridgeHostPropertyName();

  @Value.Parameter
  // example: "dw.stargate.bridge.port"
  String bridgePortPropertyName();

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

    Builder addServiceArguments(String... elements);

    Builder bridgeHostPropertyName(String bridgeHostPropertyName);

    Builder bridgePortPropertyName(String bridgePortPropertyName);

    ApiServiceParameters build();
  }
}
