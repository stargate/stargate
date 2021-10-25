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

package io.stargate.metrics.jersey.config;

/**
 * Implementation of the {@link MetricsListenerConfig} that is based on the system properties. It
 * accepts the property prefix and then defines the properties:
 *
 * <ol>
 *   <li><code>property.prefix.enabled</code> for the {@link #isEnabled()} (default true)
 *   <li><code>property.prefix.ignore_http_tags_provider</code> for the {@link
 *       #isIgnoreHttpMetricProvider()} ()} (default false)
 * </ol>
 */
public class SystemPropsMetricsListenerConfig implements MetricsListenerConfig {

  private final String propertyPrefix;

  /**
   * Default constructor.
   *
   * @param propertyPrefix Prefix of the system properties to use.
   */
  public SystemPropsMetricsListenerConfig(String propertyPrefix) {
    this.propertyPrefix = propertyPrefix;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isEnabled() {
    String value = System.getProperty(propertyWithPrefix("enabled"), "true");
    return Boolean.parseBoolean(value);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isIgnoreHttpMetricProvider() {
    String value = System.getProperty(propertyWithPrefix("ignore_http_tags_provider"), "false");
    return Boolean.parseBoolean(value);
  }

  private String propertyWithPrefix(String property) {
    return String.join(".", propertyPrefix, property);
  }
}
