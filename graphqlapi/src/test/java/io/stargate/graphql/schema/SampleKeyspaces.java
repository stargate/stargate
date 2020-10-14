package io.stargate.graphql.schema;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;

public class SampleKeyspaces {

  public static final Keyspace LIBRARY =
      ImmutableKeyspace.builder()
          .name("library")
          .addTables(
              ImmutableTable.builder()
                  .keyspace("library")
                  .name("books")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("library")
                          .table("books")
                          .name("title")
                          .type(Column.Type.Text)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("library")
                          .table("books")
                          .name("author")
                          .type(Column.Type.Text)
                          .kind(Column.Kind.Regular)
                          .build())
                  .build())
          .build();
}
