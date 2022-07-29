package io.stargate.db.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Column.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SchemaHashTest {

  @Nested
  class KeyspaceHashCode {

    @Test
    public void changeName() {
      assertThat(ImmutableKeyspace.builder().name("ks1").build().schemaHashCode())
          .isNotEqualTo(ImmutableKeyspace.builder().name("ks2").build().schemaHashCode());
    }

    @Test
    public void changeReplication() {
      assertThat(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .replication(ImmutableMap.of("class", "SimpleStrategy"))
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .replication(ImmutableMap.of("class", "NetworkTopologyStrategy"))
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeTable() {
      assertThat(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .addTables(ImmutableTable.builder().keyspace("ks").name("table1").build())
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .addTables(ImmutableTable.builder().keyspace("ks").name("table2").build())
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeUserDefinedTypes() {
      assertThat(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .addUserDefinedTypes(
                      ImmutableUserDefinedType.builder().keyspace("ks").name("udt1").build())
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableKeyspace.builder()
                  .name("ks")
                  .addUserDefinedTypes(
                      ImmutableUserDefinedType.builder().keyspace("ks").name("udt2").build())
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class TableHashCode {

    @Test
    public void changeName() {
      assertThat(ImmutableTable.builder().keyspace("ks").name("table1").build().schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder().keyspace("ks").name("table2").build().schemaHashCode());
    }

    @Test
    public void changeKeyspace() {
      assertThat(ImmutableTable.builder().keyspace("ks1").name("table").build().schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder().keyspace("ks2").name("table").build().schemaHashCode());
    }

    @Test
    public void changeComment() {
      assertThat(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .comment("comment1")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .comment("comment2")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeTtl() {
      assertThat(
              ImmutableTable.builder().keyspace("ks").name("table").ttl(1).build().schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .ttl(2)
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeColumns() {
      assertThat(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column", Kind.PartitionKey, Type.Text))
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column", Kind.PartitionKey, Type.Int))
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeIndexes() {
      CollectionIndexingType indexingType =
          ImmutableCollectionIndexingType.builder().indexValues(true).build();
      assertThat(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .addIndexes(
                      ImmutableSecondaryIndex.builder()
                          .keyspace("ks")
                          .name("index1")
                          .indexingType(indexingType)
                          .build())
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableTable.builder()
                  .keyspace("ks")
                  .name("table")
                  .addIndexes(
                      ImmutableSecondaryIndex.builder()
                          .keyspace("ks")
                          .name("index2")
                          .indexingType(indexingType)
                          .build())
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class MaterializedViewHashCode {

    @Test
    public void changeName() {
      assertThat(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table1")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table2")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeKeyspace() {
      assertThat(
              ImmutableMaterializedView.builder()
                  .keyspace("ks1")
                  .name("table")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMaterializedView.builder()
                  .keyspace("ks2")
                  .name("table")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeComment() {
      assertThat(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .comment("comment1")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .comment("comment2")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeTtl() {
      assertThat(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .ttl(1)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .ttl(2)
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeColumns() {
      assertThat(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column", Kind.PartitionKey, Type.Text))
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMaterializedView.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column", Kind.PartitionKey, Type.Int))
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class ColumnHashCode {

    @Test
    public void changeName() {
      assertThat(Column.create("column1", Kind.PartitionKey).schemaHashCode())
          .isNotEqualTo(Column.create("column2", Kind.PartitionKey).schemaHashCode());
    }

    public void changeKind() {
      assertThat(Column.create("column", Kind.PartitionKey).schemaHashCode())
          .isNotEqualTo(Column.create("column", Kind.Regular).schemaHashCode());
    }

    public void changeType() {
      assertThat(Column.create("column", Kind.PartitionKey, Type.Text).schemaHashCode())
          .isNotEqualTo(Column.create("column", Kind.PartitionKey, Type.Int).schemaHashCode());
    }

    public void changeOrder() {
      assertThat(Column.create("column", Kind.PartitionKey, Type.Text, Order.ASC).schemaHashCode())
          .isNotEqualTo(
              Column.create("column", Kind.PartitionKey, Type.Text, Order.DESC).schemaHashCode());
    }
  }

  @Nested
  class SecondaryIndexHashCode {

    @Test
    public void changeName() {
      CollectionIndexingType indexingType =
          ImmutableCollectionIndexingType.builder().indexValues(true).build();
      assertThat(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks")
                  .name("index1")
                  .indexingType(indexingType)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks")
                  .name("index2")
                  .indexingType(indexingType)
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeKeyspace() {
      CollectionIndexingType indexingType =
          ImmutableCollectionIndexingType.builder().indexValues(true).build();
      assertThat(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks1")
                  .name("index")
                  .indexingType(indexingType)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks2")
                  .name("index")
                  .indexingType(indexingType)
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeType() {
      assertThat(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks")
                  .name("index")
                  .indexingType(ImmutableCollectionIndexingType.builder().indexValues(true).build())
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableSecondaryIndex.builder()
                  .keyspace("ks")
                  .name("index")
                  .indexingType(ImmutableCollectionIndexingType.builder().indexKeys(true).build())
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class ParameterizedTypeHashCode {
    @Test
    public void differentMaps() {
      assertThat(
              ImmutableMapType.builder()
                  .addParameters(Type.Text, Type.Text)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableMapType.builder()
                  .addParameters(Type.Int, Type.Text)
                  .build()
                  .schemaHashCode());

      assertThat(ImmutableMapType.builder().isFrozen(true).build().schemaHashCode())
          .isNotEqualTo(ImmutableMapType.builder().isFrozen(false).build().schemaHashCode());
    }

    @Test
    public void differentLists() {
      assertThat(ImmutableListType.builder().addParameters(Type.Text).build().schemaHashCode())
          .isNotEqualTo(
              ImmutableListType.builder().addParameters(Type.Int).build().schemaHashCode());

      assertThat(ImmutableListType.builder().isFrozen(true).build().schemaHashCode())
          .isNotEqualTo(ImmutableListType.builder().isFrozen(false).build().schemaHashCode());
    }

    @Test
    public void differentSets() {
      assertThat(ImmutableSetType.builder().addParameters(Type.Text).build().schemaHashCode())
          .isNotEqualTo(
              ImmutableSetType.builder().addParameters(Type.Int).build().schemaHashCode());

      assertThat(ImmutableSetType.builder().isFrozen(true).build().schemaHashCode())
          .isNotEqualTo(ImmutableSetType.builder().isFrozen(false).build().schemaHashCode());
    }

    @Test
    public void differentTuples() {
      assertThat(
              ImmutableTupleType.builder()
                  .addParameters(Type.Text, Type.Text, Type.Text)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableTupleType.builder()
                  .addParameters(Type.Int, Type.Text, Type.Text)
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class UserDefinedTypeHashCode {
    @Test
    public void changeName() {
      assertThat(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table1")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table2")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeKeyspace() {
      assertThat(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks1")
                  .name("table")
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks2")
                  .name("table")
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeIsFrozen() {
      assertThat(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table")
                  .isFrozen(true)
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table")
                  .isFrozen(false)
                  .build()
                  .schemaHashCode());
    }

    @Test
    public void changeColumns() {
      assertThat(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column1", Kind.PartitionKey))
                  .build()
                  .schemaHashCode())
          .isNotEqualTo(
              ImmutableUserDefinedType.builder()
                  .keyspace("ks")
                  .name("table")
                  .addColumns(Column.create("column2", Kind.PartitionKey))
                  .build()
                  .schemaHashCode());
    }
  }

  @Nested
  class TableNameHashCode {

    @Test
    public void changeName() {
      assertThat(
              ImmutableTableName.builder().keyspace("ks").name("table1").build().schemaHashCode())
          .isNotEqualTo(
              ImmutableTableName.builder().keyspace("ks").name("table2").build().schemaHashCode());
    }

    @Test
    public void changeKeyspace() {
      assertThat(
              ImmutableTableName.builder().keyspace("ks1").name("table").build().schemaHashCode())
          .isNotEqualTo(
              ImmutableTableName.builder().keyspace("ks2").name("table").build().schemaHashCode());
    }
  }
}
