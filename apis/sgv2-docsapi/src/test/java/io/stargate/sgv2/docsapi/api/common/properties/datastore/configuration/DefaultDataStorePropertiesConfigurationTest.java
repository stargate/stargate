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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import java.util.stream.IntStream;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DefaultDataStorePropertiesConfigurationTest {

  @Inject DocumentProperties properties;

  @Inject DataStoreProperties dataStoreProperties;

  @Test
  public void dataStoreDefaults() {
    assertThat(dataStoreProperties.secondaryIndexesEnabled()).isTrue();
    assertThat(dataStoreProperties.saiEnabled()).isFalse();
    assertThat(dataStoreProperties.loggedBatchesEnabled()).isTrue();
  }

  @Test
  public void documentDefaults() {
    assertThat(properties.maxDepth()).isEqualTo(64);
    assertThat(properties.maxArrayLength()).isEqualTo(1_000_000);
    assertThat(properties.maxPageSize()).isEqualTo(20);
    assertThat(properties.maxSearchPageSize()).isEqualTo(1_000);
  }

  @Test
  public void documentTableDefaults() {
    DocumentTableProperties tableProperties = properties.tableProperties();

    assertThat(tableProperties.keyColumnName()).isEqualTo("key");
    assertThat(tableProperties.leafColumnName()).isEqualTo("leaf");
    assertThat(tableProperties.stringValueColumnName()).isEqualTo("text_value");
    assertThat(tableProperties.doubleValueColumnName()).isEqualTo("dbl_value");
    assertThat(tableProperties.booleanValueColumnName()).isEqualTo("bool_value");
    assertThat(tableProperties.pathColumnPrefix()).isEqualTo("p");
  }

  @Test
  public void documentTableColumnsDefaults() {
    DocumentTableColumns columns = properties.tableColumns();

    assertThat(columns.valueColumnNames())
        .containsExactly("leaf", "text_value", "dbl_value", "bool_value");
    assertThat(columns.pathColumnNames())
        .hasSize(64)
        .allSatisfy(p -> assertThat(p).startsWith("p"));
    assertThat(columns.pathColumnNamesList())
        .containsExactlyElementsOf(IntStream.range(0, 64).mapToObj("p%d"::formatted).toList());
  }
}
