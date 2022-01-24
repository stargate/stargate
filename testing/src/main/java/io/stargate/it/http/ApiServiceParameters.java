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
  default int servicePort() {
    return 8088;
  }

  @Value.Default
  default int metricsPort() {
    return 8088;
  }

  static Builder builder() {
    return ImmutableApiServiceParameters.builder();
  }

  interface Builder {

    Builder putSystemProperties(String key, String value);

    Builder servicePort(int port);

    Builder metricsPort(int port);

    ApiServiceParameters build();
  }
}
