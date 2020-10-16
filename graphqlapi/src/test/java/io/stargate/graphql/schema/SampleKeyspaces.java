package io.stargate.graphql.schema;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
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
                          .type(Type.Text)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("library")
                          .table("books")
                          .name("author")
                          .type(Type.Text)
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
                    .type(Type.Int)
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

  public static final Keyspace COLLECTIONS =
      ImmutableKeyspace.builder()
          .name("collections")
          .addTables(
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("pk_list_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("pk_list_table")
                          .name("l")
                          .type(Type.List.of(Type.Int).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("regular_list_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("regular_list_table")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("regular_list_table")
                          .name("l")
                          .type(Type.List.of(Type.Int))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("pk_set_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("pk_set_table")
                          .name("s")
                          .type(Type.Set.of(Type.Int).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("regular_set_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("regular_set_table")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("regular_set_table")
                          .name("s")
                          .type(Type.Set.of(Type.Int))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("pk_map_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("pk_map_table")
                          .name("m")
                          .type(Type.Map.of(Type.Int, Type.Text).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("regular_map_table")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("regular_map_table")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("regular_map_table")
                          .name("m")
                          .type(Type.Map.of(Type.Int, Type.Text))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("nested_collections")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("nested_collections")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      // map<int,list<set<text>>>
                      // This is probably overkill for most real-world scenarios, but the goal is to
                      // check that we handle nesting correctly
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("nested_collections")
                          .name("c")
                          .type(Type.Map.of(Type.Int, Type.List.of(Type.Set.of(Type.Text))))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build())
          .build();
}
