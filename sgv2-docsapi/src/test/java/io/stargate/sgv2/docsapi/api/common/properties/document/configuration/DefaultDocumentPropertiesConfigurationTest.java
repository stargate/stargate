package io.stargate.sgv2.docsapi.api.common.properties.document.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DefaultDocumentPropertiesConfigurationTest {

  @Inject DocumentProperties documentProperties;

  @Test
  public void documentDefaults() {
    assertThat(documentProperties.maxDepth()).isEqualTo(64);
    assertThat(documentProperties.maxArrayLength()).isEqualTo(1_000_000);
    assertThat(documentProperties.maxPageSize()).isEqualTo(20);
    assertThat(documentProperties.searchPageSize()).isEqualTo(1_000);
  }

  @Test
  public void documentTableDefaults() {
    DocumentTableProperties tableProperties = documentProperties.tableProperties();

    assertThat(tableProperties.keyColumnName()).isEqualTo("key");
    assertThat(tableProperties.leafColumnName()).isEqualTo("leaf");
    assertThat(tableProperties.stringValueColumnName()).isEqualTo("text_value");
    assertThat(tableProperties.doubleValueColumnName()).isEqualTo("dbl_value");
    assertThat(tableProperties.booleanValueColumnName()).isEqualTo("bool_value");
    assertThat(tableProperties.pathColumnPrefix()).isEqualTo("p");
  }

  @Test
  public void documentTableColumnsDefaults() {
    DocumentTableColumns columns = documentProperties.tableColumns();

    assertThat(columns.valueColumnNames())
        .containsExactly("leaf", "text_value", "dbl_value", "bool_value");
    assertThat(columns.pathColumnNames())
        .hasSize(64)
        .allSatisfy(p -> assertThat(p).startsWith("p"));
    assertThat(columns.allColumnNames())
        .hasSize(69)
        .contains("key")
        .containsAll(columns.valueColumnNames())
        .containsAll(columns.pathColumnNames());
  }
}
