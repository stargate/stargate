package io.stargate.it.cql.compatibility.protocolv4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(customBuilder = "registerListener", customOptions = "applyProtocolVersion")
public class SchemaChangesTest extends BaseIntegrationTest {

  private static SchemaChangeListener schemaChanges;
  @Captor ArgumentCaptor<TableMetadata> tableCaptor;
  @Captor ArgumentCaptor<TableMetadata> previousTableCaptor;

  public static CqlSessionBuilder registerListener(CqlSessionBuilder builder) {
    schemaChanges = Mockito.mock(SchemaChangeListener.class);
    return builder.withSchemaChangeListener(schemaChanges);
  }

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName("Should notify of schema changes")
  public void schemaChangesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
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

  @Test
  @DisplayName("Should fail when trying to create schema elements that already exist")
  public void alreadyExistsErrors(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    assertThatThrownBy(
            () ->
                session.execute(
                    String.format(
                        "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                        keyspaceId.asCql(false))))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Keyspace ")
        .hasMessageContaining("already exists");

    session.execute("CREATE TABLE foo(k int PRIMARY KEY)");
    assertThatThrownBy(() -> session.execute("CREATE TABLE foo(k int PRIMARY KEY)"))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Object ")
        .hasMessageContaining("already exists");

    session.execute("CREATE TYPE t(i int)");
    assertThatThrownBy(() -> session.execute("CREATE TYPE t(i int)"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("A user type ")
        .hasMessageContaining("already exists");
  }
}
