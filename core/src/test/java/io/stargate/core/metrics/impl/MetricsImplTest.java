/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.core.metrics.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MetricsImplTest {

  @Test
  public void ensureDropwizardWrapped() {
    MetricsImpl metrics = new MetricsImpl();

    metrics.getRegistry().counter("dropwizard").inc();
    metrics.getRegistry("prefix").counter("dropwizard").inc();
    metrics.getMeterRegistry().counter("micrometer").increment(2d);
    metrics.getMeterRegistry().counter("micrometer_tags", "tag", "that").increment(3d);

    String scrape = metrics.scrape();
    assertThat(scrape)
        .contains("dropwizard 1.0")
        .contains("prefix_dropwizard 1.0")
        .contains("micrometer_total 2.0")
        .contains("micrometer_tags_total{tag=\"that\",} 3.0");
  }
}
