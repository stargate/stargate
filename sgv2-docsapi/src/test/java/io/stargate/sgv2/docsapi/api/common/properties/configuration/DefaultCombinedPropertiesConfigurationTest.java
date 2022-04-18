package io.stargate.sgv2.docsapi.api.common.properties.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.docsapi.api.common.properties.model.CombinedProperties;
import io.stargate.sgv2.docsapi.api.common.properties.model.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.model.DocumentTableProperties;
import java.util.Arrays;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DefaultCombinedPropertiesConfigurationTest {

  @Inject CombinedProperties properties;

  @Test
  public void dataStoreDefaults() {
    assertThat(properties.secondaryIndexesEnabled()).isTrue();
    assertThat(properties.saiEnabled()).isFalse();
    assertThat(properties.loggedBatchesEnabled()).isTrue();
  }

  @Test
  public void documentDefaults() {
    assertThat(properties.maxDepth()).isEqualTo(64);
    assertThat(properties.maxArrayLength()).isEqualTo(1_000_000);
    assertThat(properties.maxPageSize()).isEqualTo(20);
    assertThat(properties.searchPageSize()).isEqualTo(1_000);
  }

  @Test
  public void documentTableDefaults() {
    DocumentTableProperties tableProperties = properties.table();

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
    assertThat(columns.allColumnNames())
        .hasSize(69)
        .contains("key")
        .containsAll(Arrays.asList(columns.valueColumnNames()))
        .containsAll(Arrays.asList(columns.pathColumnNames()));
  }
}
