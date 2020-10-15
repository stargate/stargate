package io.stargate.graphql.schema;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.UserDefinedType;

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

  public static final Keyspace UDTS = buildUdts();

  private static ImmutableKeyspace buildUdts() {
    UserDefinedType bType =
        ImmutableUserDefinedType.builder()
            .keyspace("udts")
            .name("b")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("udts")
                    .table("b")
                    .name("i")
                    .type(Column.Type.Int)
                    .kind(Column.Kind.Regular)
                    .build())
            .build();
    UserDefinedType aType =
        ImmutableUserDefinedType.builder()
            .keyspace("udts")
            .name("a")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("udts")
                    .table("a")
                    .name("b")
                    .type(bType.frozen(true))
                    .kind(Column.Kind.Regular)
                    .build())
            .build();
    return ImmutableKeyspace.builder()
        .name("udts")
        .addUserDefinedTypes(aType, bType)
        .addTables(
            ImmutableTable.builder()
                .keyspace("udts")
                .name("test_table")
                .addColumns(
                    ImmutableColumn.builder()
                        .keyspace("udts")
                        .table("test_table")
                        .name("a")
                        .type(aType.frozen(true))
                        .kind(Column.Kind.PartitionKey)
                        .build())
                .build())
        .build();
  }
}
