package io.stargate.db.cassandra.impl;

import io.stargate.db.cassandra.datastore.DataStoreUtil;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.MaterializedView;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.stargate.utils.Streams;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods handling the conversion between internal C* schema classes and Stargate schema
 * ones.
 */
class SchemaConversions {
  private static final Logger logger = LoggerFactory.getLogger(SchemaConversions.class);

  static Schema convertCassandraSchema(Stream<KeyspaceMetadata> cassandraKeyspaces) {
    return Schema.create(cassandraKeyspaces.map(SchemaConversions::convertKeyspace)::iterator);
  }

  static Keyspace convertKeyspace(KeyspaceMetadata keyspace) {
    Stream<Table> tables = convertTables(keyspace.tables, keyspace.views);
    Stream<UserDefinedType> userDefinedTypes = convertUserTypes(keyspace.name, keyspace.types);
    return Keyspace.create(
        keyspace.name,
        tables::iterator,
        userDefinedTypes::iterator,
        keyspace.params.replication.asMap(),
        Optional.of(keyspace.params.durableWrites));
  }

  // We pass the MVs because in Stargate metadata, each table lists the MVs for which it is a base
  // table as an index.
  private static Stream<Table> convertTables(
      Iterable<CFMetaData> tables, Iterable<ViewDefinition> views) {
    return Streams.of(tables).map(t -> convertTable(t, views));
  }

  private static Table convertTable(CFMetaData table, Iterable<ViewDefinition> views) {
    List<Column> columns = convertColumns(table).collect(Collectors.toList());
    Stream<Index> secondaryIndexes = convertSecondaryIndexes(table, columns);
    Stream<Index> materializedViews = convertMVIndexes(table, views);
    return Table.create(
        table.ksName,
        table.cfName,
        columns,
        Stream.concat(secondaryIndexes, materializedViews)::iterator);
  }

  private static Stream<Column> convertColumns(CFMetaData table) {
    return convertColumns(table::allColumnsInSelectOrder);
  }

  private static Stream<Column> convertColumns(Iterable<ColumnDefinition> columns) {
    return Streams.of(columns).map(SchemaConversions::convertColumn);
  }

  private static Column convertColumn(ColumnDefinition column) {
    return ImmutableColumn.builder()
        .name(column.name.toString())
        .type(DataStoreUtil.getTypeFromInternal(column.type))
        .kind(convertColumnKind(column.kind))
        .order(convertClusteringOrder(column.clusteringOrder()))
        .build();
  }

  private static Column.Order convertClusteringOrder(
      ColumnDefinition.ClusteringOrder clusteringOrder) {
    switch (clusteringOrder) {
      case ASC:
        return Column.Order.Asc;
      case DESC:
        return Column.Order.Desc;
      case NONE:
        return null;
      default:
        throw new IllegalStateException("Clustering columns should always have an order");
    }
  }

  private static Column.Kind convertColumnKind(ColumnDefinition.Kind kind) {
    switch (kind) {
      case REGULAR:
        return Column.Kind.Regular;
      case STATIC:
        return Column.Kind.Static;
      case PARTITION_KEY:
        return Column.Kind.PartitionKey;
      case CLUSTERING:
        return Column.Kind.Clustering;
    }
    throw new IllegalStateException("Unknown column kind");
  }

  private static Stream<Index> convertSecondaryIndexes(CFMetaData table, List<Column> columns) {
    // This is an unfortunate departure from the rest of this class being 'stateless'. This
    // is really a weakness of the C* schema code that some of the index information is not
    // part of schema objects and that we should reach into the ColumnFamilyStore, but there is
    // nothing we can do at the Stargate level...
    return org.apache.cassandra.db.Keyspace.openAndGetStore(table).indexManager.listIndexes()
        .stream()
        .map(
            i ->
                (Index)
                    convertSecondaryIndex(
                        table.ksName, table.cfName, i.getIndexMetadata(), columns))
        .filter(Objects::nonNull);
  }

  private static SecondaryIndex convertSecondaryIndex(
      String keyspaceName, String tableName, IndexMetadata index, List<Column> baseTableColumns) {
    String cassandraTargetColumn = index.options.get("target");
    if (cassandraTargetColumn == null) {
      logger.error(
          "No 'target' defined for index {} on {}.{}. This should not happen and should "
              + "be reported for investigation. That index will not be visible in Stargate schema view",
          index.name,
          keyspaceName,
          tableName);
      return null;
    }
    Pair<String, CollectionIndexingType> result = convertTargetColumn(index.options.get("target"));
    String targetColumn = result.getValue0();
    Optional<Column> col =
        baseTableColumns.stream().filter(c -> c.name().equals(targetColumn)).findFirst();
    if (!col.isPresent()) {
      logger.error(
          "Could not find secondary index target column {} in the columns of table {}.{} "
              + "({}). This should not happen and should be reported for investigation. That index "
              + "will not be visible in Stargate schema view.",
          targetColumn,
          keyspaceName,
          tableName,
          baseTableColumns);
      return null;
    }
    return SecondaryIndex.create(keyspaceName, index.name, col.get(), result.getValue1());
  }

  /**
   * Using a secondary index on a column X where X is a map and depending on the type of indexing,
   * such as KEYS(X) / VALUES(X) / ENTRIES(X), the actual column name will be wrapped, and we're
   * trying to extract that column name here.
   *
   * @param targetColumn the target column as in Cassandra.
   * @return A {@link Pair} containing the actual target column name as first parameter and the
   *     {@link CollectionIndexingType} as the second parameter.
   */
  private static Pair<String, CollectionIndexingType> convertTargetColumn(String targetColumn) {
    if (targetColumn.startsWith("values(")) {
      return new Pair<>(
          targetColumn.replace("values(", "").replace(")", "").replace("\"", ""),
          ImmutableCollectionIndexingType.builder().indexValues(true).build());
    }
    if (targetColumn.startsWith("keys(")) {
      return new Pair<>(
          targetColumn.replace("keys(", "").replace(")", "").replace("\"", ""),
          ImmutableCollectionIndexingType.builder().indexKeys(true).build());
    }
    if (targetColumn.startsWith("entries(")) {
      return new Pair<>(
          targetColumn.replace("entries(", "").replace(")", "").replace("\"", ""),
          ImmutableCollectionIndexingType.builder().indexEntries(true).build());
    }
    if (targetColumn.startsWith("full(")) {
      return new Pair<>(
          targetColumn.replace("full(", "").replace(")", "").replace("\"", ""),
          ImmutableCollectionIndexingType.builder().indexFull(true).build());
    }
    return new Pair<>(
        targetColumn.replaceAll("\"", ""), ImmutableCollectionIndexingType.builder().build());
  }

  // Converts the MVs _belonging to the provided table_ (ignoring other ones).
  private static Stream<Index> convertMVIndexes(CFMetaData table, Iterable<ViewDefinition> views) {
    return Streams.of(views)
        .filter(v -> v.baseTableId.equals(table.cfId))
        .map(SchemaConversions::convertMVIndex);
  }

  private static Index convertMVIndex(ViewDefinition view) {
    Stream<Column> columns = convertColumns(view.metadata);
    return MaterializedView.create(view.ksName, view.metadata.cfName, columns::iterator);
  }

  private static Stream<UserDefinedType> convertUserTypes(
      String keyspaceName, Iterable<UserType> userTypes) {
    return Streams.of(userTypes)
        .map(userType -> SchemaConversions.convertUserType(keyspaceName, userType));
  }

  private static UserDefinedType convertUserType(String keyspaceName, UserType userType) {
    List<Column> columns = DataStoreUtil.getUDTColumns(userType);
    return ImmutableUserDefinedType.builder()
        .keyspace(keyspaceName)
        .name(userType.getNameAsString())
        .columns(columns)
        .build();
  }
}
