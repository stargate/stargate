package io.stargate.db.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test(s) to verify that calculation of Schema hash codes is only based on content and does not
 * depend on system being run on -- that is, value does not change from run to run or system to
 * system. Test will break if hash calculation changes in any way; new "known" hash code values need
 * to be inserted. This is by design: expectation being that such changes are rare.
 *
 * <p>Test was added after it was found that Schema hash calculation produced different values on
 * different Coordinator nodes; this test would have caught that particular problem.
 */
public class SchemaHashStabilityTest {
  private static final int COLUMNS_HASH = -1617312245;

  private static final int INDEXES_HASH = -515172221;

  private static final int KEYSPACE_HASH = 1487526822;

  private static final int TABLE_HASH = -1473268881;

  @Test
  public void validateKeyspaceHash() {
    assertThat(buildKeyspace().schemaHashCode()).isEqualTo(KEYSPACE_HASH);
  }

  @Test
  public void validateTableHash() {
    Keyspace ks = buildKeyspace();
    Column countryCol =
        Column.create(
            "current_country",
            Column.Type.Tuple.frozen().of(Column.Type.Ascii, Column.Type.Date, Column.Type.Date));
    Column evaluationsCol =
        Column.create(
            "evaluations",
            Column.Kind.Regular,
            Column.Type.Map.of(Column.Type.Int, Column.Type.Ascii));
    Column favorite_books =
        Column.create("favorite_books", Column.Kind.Regular, Column.Type.Set.of(Column.Type.Ascii));
    Column top3ShowsCol =
        Column.create(
            "top_three_tv_shows", Column.Kind.Regular, Column.Type.List.of(Column.Type.Ascii));
    Column[] columns =
        new Column[] {
          Column.create("firstname", Column.Kind.PartitionKey, Column.Type.Ascii),
          Column.create("lastname", Column.Kind.Clustering, Column.Type.Ascii, Column.Order.ASC),
          Column.create("address", Column.Type.fromCqlDefinitionOf(ks, "address_type")),
          countryCol,
          Column.create("email", Column.Kind.Regular, Column.Type.Ascii),
          evaluationsCol,
          favorite_books,
          Column.create("favorite_color", Column.Kind.Regular, Column.Type.Ascii),
          top3ShowsCol
        };

    // Validate hash of all columns first
    assertThat(SchemaHashable.hash(Arrays.asList(columns))).isEqualTo(COLUMNS_HASH);

    // Then those of all indexes
    Index[] indexes =
        new Index[] {
          ImmutableSecondaryIndex.create("users_keyspace", "country_idx", countryCol),
          ImmutableSecondaryIndex.builder()
              .keyspace("users_keyspace")
              .name("evale_idx")
              .column(evaluationsCol)
              .indexingType(ImmutableCollectionIndexingType.builder().indexEntries(true).build())
              .isCustom(true)
              .indexingClass("org.apache.cassandra.index.sai.StorageAttachedIndex")
              .build(),
          ImmutableSecondaryIndex.builder()
              .keyspace("users_keyspace")
              .name("evalk_idx")
              .column(evaluationsCol)
              .indexingType(ImmutableCollectionIndexingType.builder().indexKeys(true).build())
              .isCustom(true)
              .indexingClass("org.apache.cassandra.index.sai.StorageAttachedIndex")
              .build(),
          ImmutableSecondaryIndex.builder()
              .keyspace("users_keyspace")
              .name("evalv_idx")
              .column(evaluationsCol)
              .indexingType(ImmutableCollectionIndexingType.builder().indexValues(true).build())
              .isCustom(true)
              .indexingClass("org.apache.cassandra.index.sai.StorageAttachedIndex")
              .build(),
          ImmutableSecondaryIndex.builder()
              .keyspace("users_keyspace")
              .name("fav_books_idx")
              .column(favorite_books)
              .indexingType(ImmutableCollectionIndexingType.builder().indexValues(true).build())
              .isCustom(true)
              .indexingClass("org.apache.cassandra.index.sai.StorageAttachedIndex")
              .build(),
          ImmutableSecondaryIndex.builder()
              .keyspace("users_keyspace")
              .name("tv_idx")
              .column(top3ShowsCol)
              .indexingType(ImmutableCollectionIndexingType.builder().indexValues(true).build())
              .isCustom(true)
              .indexingClass("org.apache.cassandra.index.sai.StorageAttachedIndex")
              .build()
        };
    assertThat(SchemaHashable.hash(Arrays.asList(indexes))).isEqualTo(INDEXES_HASH);

    // And then resulting table

    Table table =
        ImmutableTable.builder()
            .keyspace("users_keyspace")
            .name("users")
            .addColumns(columns)
            .addIndexes(indexes)
            .build();
    assertThat(table.schemaHashCode()).isEqualTo(TABLE_HASH);
  }

  private Keyspace buildKeyspace() {
    Map<String, String> keyspaceReplication = Map.of("class", "SimpleStrategy", "eu-west-1", "3");
    return ImmutableKeyspace.builder()
        .name("users_keyspace")
        .replication(keyspaceReplication)
        .durableWrites(true)
        .addUserDefinedTypes(
            ImmutableUserDefinedType.builder()
                .keyspace("users_keyspace")
                .name("address_type")
                .addColumns(
                    Column.create("street", Column.Type.Ascii),
                    Column.create("city", Column.Type.Ascii),
                    Column.create("state", Column.Type.Ascii),
                    Column.create("zip", Column.Type.Ascii))
                .build())
        .build();
  }
}
