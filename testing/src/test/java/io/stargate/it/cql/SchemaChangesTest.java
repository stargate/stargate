package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SchemaChangesTest extends JavaDriverTestBase {

  @Mock private SchemaChangeListener schemaChanges;
  @Captor ArgumentCaptor<TableMetadata> tableCaptor;
  @Captor ArgumentCaptor<TableMetadata> previousTableCaptor;

  public SchemaChangesTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Override
  protected void customizeBuilder(CqlSessionBuilder builder) {
    assertThat(schemaChanges).isNotNull();
    builder.withSchemaChangeListener(schemaChanges);
  }

  @Test
  public void should_notify_of_schema_changes() {
    // This is not an extensive coverage of the driver's schema metadata, we just want to check that
    // the schema events on the control connection are wired correctly.
    session.execute("CREATE TABLE foo(k int PRIMARY KEY)");
    verify(schemaChanges).onTableCreated(tableCaptor.capture());
    TableMetadata createdTable = tableCaptor.getValue();
    assertThat(createdTable.getName().asInternal()).isEqualTo("foo");

    assertThat(session.getMetadata().getKeyspace(keyspaceId).flatMap(ks -> ks.getTable("foo")))
        .hasValue(createdTable);

    session.execute("ALTER TABLE foo ADD v int");
    verify(schemaChanges).onTableUpdated(tableCaptor.capture(), previousTableCaptor.capture());
    TableMetadata previousTable = previousTableCaptor.getValue();
    assertThat(previousTable).isEqualTo(createdTable);
    TableMetadata updatedTable = tableCaptor.getValue();
    assertThat(updatedTable.getName().asInternal()).isEqualTo("foo");
    assertThat(updatedTable.getColumns().keySet())
        .containsExactly(CqlIdentifier.fromInternal("k"), CqlIdentifier.fromInternal("v"));

    assertThat(session.getMetadata().getKeyspace(keyspaceId).flatMap(ks -> ks.getTable("foo")))
        .hasValue(updatedTable);

    session.execute("DROP TABLE foo");
    verify(schemaChanges).onTableDropped(tableCaptor.capture());
    TableMetadata droppedTable = tableCaptor.getValue();
    assertThat(droppedTable.getName().asInternal()).isEqualTo("foo");

    assertThat(session.getMetadata().getKeyspace(keyspaceId))
        .hasValueSatisfying(ks -> assertThat(ks.getTable("foo")).isEmpty());
  }
}
