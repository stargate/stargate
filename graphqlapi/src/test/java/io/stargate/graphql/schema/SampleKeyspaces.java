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
          .addTables(
              ImmutableTable.builder()
                  .keyspace("library")
                  .name("authors")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("library")
                          .table("books")
                          .name("author")
                          .type(Column.Type.Text)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("library")
                          .table("books")
                          .name("title")
                          .type(Column.Type.Text)
                          .kind(Column.Kind.Clustering)
                          .build())
                  .build())
          .build();

  public static final Keyspace UDTS = buildUdts();

  private static ImmutableKeyspace buildUdts() {
    UserDefinedType bType =
        ImmutableUserDefinedType.builder()
            .keyspace("udts")
            .name("B")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("udts")
                    .table("B")
                    .name("i")
                    .type(Type.Int)
                    .kind(Column.Kind.Regular)
                    .build())
            .build();
    UserDefinedType aType =
        ImmutableUserDefinedType.builder()
            .keyspace("udts")
            .name("A")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("udts")
                    .table("A")
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
                .name("TestTable")
                .addColumns(
                    ImmutableColumn.builder()
                        .keyspace("udts")
                        .table("TestTable")
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
                  .name("PkListTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("PkListTable")
                          .name("l")
                          .type(Type.List.of(Type.Int).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("RegularListTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("RegularListTable")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("RegularListTable")
                          .name("l")
                          .type(Type.List.of(Type.Int))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("PkSetTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("PkSetTable")
                          .name("s")
                          .type(Type.Set.of(Type.Int).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("RegularSetTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("RegularSetTable")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("RegularSetTable")
                          .name("s")
                          .type(Type.Set.of(Type.Int))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("PkMapTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .table("PkMapTable")
                          .name("m")
                          .type(Type.Map.of(Type.Int, Type.Text).frozen())
                          .kind(Column.Kind.PartitionKey)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("RegularMapTable")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("RegularMapTable")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("RegularMapTable")
                          .name("m")
                          .type(Type.Map.of(Type.Int, Type.Text))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build(),
              ImmutableTable.builder()
                  .keyspace("collections")
                  .name("NestedCollections")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("NestedCollections")
                          .name("k")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      // map<int,list<set<text>>>
                      // This is probably overkill for most real-world scenarios, but the goal is to
                      // check that we handle nesting correctly
                      ImmutableColumn.builder()
                          .keyspace("collections")
                          .name("NestedCollections")
                          .name("c")
                          .type(Type.Map.of(Type.Int, Type.List.of(Type.Set.of(Type.Text))))
                          .kind(Column.Kind.Regular)
                          .build())
                  .build())
          .build();

  public static final Keyspace IOT =
      ImmutableKeyspace.builder()
          .name("iot")
          .addTables(
              ImmutableTable.builder()
                  .keyspace("iot")
                  .name("readings")
                  .addColumns(
                      ImmutableColumn.builder()
                          .keyspace("iot")
                          .table("readings")
                          .name("id")
                          .type(Type.Int)
                          .kind(Column.Kind.PartitionKey)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("iot")
                          .table("readings")
                          .name("year")
                          .type(Type.Int)
                          .kind(Column.Kind.Clustering)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("iot")
                          .table("readings")
                          .name("month")
                          .type(Type.Int)
                          .kind(Column.Kind.Clustering)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("iot")
                          .table("readings")
                          .name("day")
                          .type(Type.Int)
                          .kind(Column.Kind.Clustering)
                          .build(),
                      ImmutableColumn.builder()
                          .keyspace("iot")
                          .table("readings")
                          .name("value")
                          .type(Type.Float)
                          .kind(Column.Kind.Clustering)
                          .build())
                  .build())
          .build();
}
