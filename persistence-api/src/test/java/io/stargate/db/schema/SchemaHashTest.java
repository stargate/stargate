package io.stargate.db.schema;

import org.junit.jupiter.api.Test;

public class SchemaHashTest {

  @Test
  public void keyspaceTest() {
    Keyspace ks =
        ImmutableKeyspace.builder()
            .name("ks")
            .addTables(ImmutableTable.builder().keyspace("ks").name("table").build())
            .build();
    int hash = ks.schemaHashCode();
  }
}
