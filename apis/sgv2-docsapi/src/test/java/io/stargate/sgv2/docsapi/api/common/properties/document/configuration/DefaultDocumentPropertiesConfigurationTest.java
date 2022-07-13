package io.stargate.sgv2.docsapi.api.common.properties.document.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import java.util.stream.IntStream;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
class DefaultDocumentPropertiesConfigurationTest {

  @Inject DocumentProperties documentProperties;

  @Test
  public void documentDefaults() {
    assertThat(documentProperties.maxDepth()).isEqualTo(64);
    assertThat(documentProperties.maxArrayLength()).isEqualTo(1_000_000);
    assertThat(documentProperties.maxPageSize()).isEqualTo(20);
    assertThat(documentProperties.maxSearchPageSize()).isEqualTo(1_000);
  }

  @Nested
  class TableProperties {

    @Test
    public void defaults() {
      DocumentTableProperties tableProperties = documentProperties.tableProperties();

      assertThat(tableProperties.keyColumnName()).isEqualTo("key");
      assertThat(tableProperties.leafColumnName()).isEqualTo("leaf");
      assertThat(tableProperties.stringValueColumnName()).isEqualTo("text_value");
      assertThat(tableProperties.doubleValueColumnName()).isEqualTo("dbl_value");
      assertThat(tableProperties.booleanValueColumnName()).isEqualTo("bool_value");
      assertThat(tableProperties.pathColumnPrefix()).isEqualTo("p");
    }
  }

  @Nested
  class TableColumns {

    @Test
    public void defaults() {
      DocumentTableColumns columns = documentProperties.tableColumns();

      assertThat(columns.valueColumnNames())
          .containsExactly("leaf", "text_value", "dbl_value", "bool_value");
      assertThat(columns.pathColumnNames())
          .hasSize(64)
          .allSatisfy(p -> assertThat(p).startsWith("p"));
      assertThat(columns.pathColumnNamesList())
          .containsExactlyElementsOf(IntStream.range(0, 64).mapToObj("p%d"::formatted).toList());
      assertThat(columns.allColumnNamesArray())
          .containsAll(columns.allColumns().stream().map(Column::name).toList());
    }

    @Test
    public void referenceEquals() {
      DocumentTableColumns columns = documentProperties.tableColumns();

      assertThat(columns.allColumns()).isSameAs(columns.allColumns());
      assertThat(columns.allColumnNamesArray()).isSameAs(columns.allColumnNamesArray());
      assertThat(columns.valueColumnNames()).isSameAs(columns.valueColumnNames());
      assertThat(columns.pathColumnNames()).isSameAs(columns.pathColumnNames());
      assertThat(columns.pathColumnNamesList()).isSameAs(columns.pathColumnNamesList());
    }

    @Test
    public void allColumnNamesWithPathDepth() {
      DocumentTableColumns columns = documentProperties.tableColumns();

      assertThat(columns.allColumnNamesWithPathDepth(0))
          .doesNotContainAnyElementsOf(columns.pathColumnNames());
      assertThat(columns.allColumnNamesWithPathDepth(2))
          .contains(documentProperties.tableProperties().keyColumnName())
          .containsAll(columns.valueColumnNames())
          .contains("p0", "p1")
          .doesNotContainAnyElementsOf(
              columns.pathColumnNamesList().subList(2, documentProperties.maxDepth()));
    }
  }
}
