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
package io.stargate.it.storage;

import java.util.Collections;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Defines parameters for each of the Stargate nodes requested by a tests.
 *
 * @see StargateSpec#parametersCustomizer()
 */
@Value.Immutable(prehash = true)
public interface StargateParameters {

  @Value.Default
  default boolean enableAuth() {
    return false;
  }

  @Value.Default
  default Map<String, String> systemProperties() {
    return Collections.emptyMap();
  }

  @Value.Default
  default boolean useProxyProtocol() {
    return false;
  }

  @Value.Default
  default String proxyDnsName() {
    return "stargate.local";
  }

  @Value.Default
  default int proxyPort() {
    return 9043;
  }

  static Builder builder() {
    return ImmutableStargateParameters.builder();
  }

  interface Builder {
    Builder enableAuth(boolean auth);

    Builder putSystemProperties(String key, String value);

    Builder useProxyProtocol(boolean enabled);

    Builder proxyDnsName(String name);

    Builder proxyPort(int port);

    StargateParameters build();
  }
}
