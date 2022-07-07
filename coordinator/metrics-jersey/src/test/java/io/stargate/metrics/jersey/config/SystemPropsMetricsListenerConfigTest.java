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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SystemPropsMetricsListenerConfigTest {

  @AfterEach
  public void clear() {
    System.clearProperty("some.prop.enabled");
    System.clearProperty("some.prop.ignore_http_tags_provider");
  }

  @Nested
  class IsEnabled {

    @Test
    public void defaults() {
      SystemPropsMetricsListenerConfig config = new SystemPropsMetricsListenerConfig("some.prop");

      boolean result = config.isEnabled();

      assertThat(result).isTrue();
    }

    @Test
    public void override() {
      System.setProperty("some.prop.enabled", "false");
      SystemPropsMetricsListenerConfig config = new SystemPropsMetricsListenerConfig("some.prop");

      boolean result = config.isEnabled();

      assertThat(result).isFalse();
    }
  }

  @Nested
  class IsIgnoreHttpMetricProvider {

    @Test
    public void defaults() {
      SystemPropsMetricsListenerConfig config = new SystemPropsMetricsListenerConfig("some.prop");

      boolean result = config.isIgnoreHttpMetricProvider();

      assertThat(result).isFalse();
    }

    @Test
    public void override() {
      System.setProperty("some.prop.ignore_http_tags_provider", "true");
      SystemPropsMetricsListenerConfig config = new SystemPropsMetricsListenerConfig("some.prop");

      boolean result = config.isEnabled();

      assertThat(result).isTrue();
    }
  }
}
