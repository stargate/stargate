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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DefaultDataStorePropertiesConfigurationTest {

  @Inject DataStoreProperties dataStoreProperties;

  @Test
  public void dataStoreDefaults() {
    assertThat(dataStoreProperties.secondaryIndexesEnabled()).isTrue();
    assertThat(dataStoreProperties.saiEnabled()).isFalse();
    assertThat(dataStoreProperties.loggedBatchesEnabled()).isTrue();
  }
}
