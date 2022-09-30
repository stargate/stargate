package io.stargate.db.datastore.common;

import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.utils.Streams;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps, for a Cassandra-like persistence module, the writing of a converter between internal
 * schema classes and Stargate schema ones.
 *
 * <p>Note that this class has no interesting logic, as it cannot make much assumption on the
 * concrete classes of the schema entities in the persistence layer. Its only goal is to save some
 * boilerplate code when writing such converter.
 *
 * @param <K> the concrete class for keyspace metadata in the persistence layer.
 * @param <T> the concrete class for table metadata in the persistence layer.
 * @param <C> the concrete class for column metadata in the persistence layer.
 * @param <U> the concrete class for user types in the persistence layer.
 * @param <I> the concrete class for secondary indexes metadata in the persistence layer.
 * @param <V> the concrete class for materialized views metadata in the persistence layer.
 */
public abstract class AbstractCassandraSchemaConverter<K, T, C, U, I, V> {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractCassandraSchemaConverter.class);

  /** The excluded name of the provided internal keyspace. */
  protected abstract Set<String> getExcludedIndexOptions();

  /** The (unquoted) name of the provided internal keyspace. */
  protected abstract String keyspaceName(K keyspace);

  /** The replication options of the provided internal keyspace. */
  protected abstract Map<String, String> replicationOptions(K keyspace);

  /** Whether the provided internal keyspace uses durable writes. */
  protected abstract boolean usesDurableWrites(K keyspace);

  /** The internal tables of the provided internal keyspace. */
  protected abstract Iterable<T> tables(K keyspace);

  /** The internal user types of the provided internal keyspace. */
  protected abstract Iterable<U> userTypes(K keyspace);

  /** The internal materialized views of the provided internal keyspace. */
  protected abstract Iterable<V> views(K keyspace);

  /** The (unquoted) name of the provided internal table. */
  protected abstract String tableName(T table);

  /** The internal columns of the internal table. */
  protected abstract Iterable<C> columns(T table);

  /** The (unquoted) name of the provided internal column. */
  protected abstract String columnName(C column);

  /** The type of the provided internal column. */
  protected abstract Column.ColumnType columnType(C column);

  /** The clustering order of the provided internal column; */
  protected abstract Column.Order columnClusteringOrder(C column);

  /** The kind of the provided internal column. */
  protected abstract Column.Kind columnKind(C column);

  /** The (internal) 2ndary indexes of provided internal table. */
  protected abstract Iterable<I> secondaryIndexes(T table);

  /** The comment on the provided internal table. * */
  protected abstract String comment(T table);

  /** The TTL on the provided internal table. * */
  protected abstract int ttl(T table);

  /** The (unquoted) name of the provided internal index. */
  protected abstract String indexName(I index);

  /** The target of the provided internal index, that is the content of it's "target" option. */
  protected abstract String indexTarget(I index);

  /** Whether the given index is of the "CUSTOM" kind. */
  protected abstract boolean isCustom(I index);

  /** The index class of the given custom index */
  protected abstract String indexClass(I index);

  /** The index options of the given custom index */
  protected abstract Map<String, String> indexOptions(I index);

  /**
   * Returns the fields of the provided internal user type as columns (of kind {@link
   * Column.Kind#Regular}).
   */
  protected abstract List<Column> userTypeFields(U userType);

  /** The name of the provided internal user type. */
  protected abstract String userTypeName(U userType);

  /** The internal "table" metadata class of the provided internal materialized view. */
  protected abstract T asTable(V view);

  /** Whether the provided table is the base table of the provided view. */
  protected abstract boolean isBaseTableOf(T table, V view);

  public Schema convertCassandraSchema(Iterable<K> cassandraKeyspaces) {
    return Schema.create(Iterables.transform(cassandraKeyspaces, this::convertKeyspace));
  }

  private Keyspace convertKeyspace(K keyspace) {
    String name = keyspaceName(keyspace);
    Stream<Table> tables = convertTables(name, tables(keyspace), views(keyspace));
    Stream<UserDefinedType> userDefinedTypes = convertUserTypes(name, userTypes(keyspace));
    return Keyspace.create(
        name,
        tables.collect(Collectors.toList()),
        userDefinedTypes.collect(Collectors.toList()),
        replicationOptions(keyspace),
        Optional.of(usesDurableWrites(keyspace)));
  }

  // We pass the MVs because in Stargate metadata, each table lists the MVs for which it is a base
  // table as an index.
  private Stream<Table> convertTables(String keyspaceName, Iterable<T> tables, Iterable<V> views) {
    return Streams.of(tables).map(t -> convertTable(keyspaceName, t, views));
  }

  private Table convertTable(String keyspaceName, T table, Iterable<V> views) {
    List<Column> columns = convertColumns(keyspaceName, table).collect(Collectors.toList());
    Stream<Index> secondaryIndexes = convertSecondaryIndexes(keyspaceName, table, columns);
    Stream<Index> materializedViews = convertMVIndexes(keyspaceName, table, views);
    String comment = comment(table);
    int ttl = ttl(table);
    return Table.create(
        keyspaceName,
        tableName(table),
        columns,
        Stream.concat(secondaryIndexes, materializedViews).collect(Collectors.toList()),
        comment,
        ttl);
  }

  private Stream<Column> convertColumns(String keyspaceName, T table) {
    return convertColumns(keyspaceName, tableName(table), columns(table));
  }

  private Stream<Column> convertColumns(
      String keyspaceName, String tableName, Iterable<C> columns) {
    return Streams.of(columns).map(c -> convertColumn(keyspaceName, tableName, c));
  }

  private Column convertColumn(String keyspaceName, String tableName, C column) {
    return ImmutableColumn.builder()
        .name(columnName(column))
        .keyspace(keyspaceName)
        .table(tableName)
        .type(columnType(column))
        .kind(columnKind(column))
        .order(columnClusteringOrder(column))
        .build();
  }

  private Stream<Index> convertSecondaryIndexes(
      String keyspaceName, T table, List<Column> columns) {
    return Streams.of(secondaryIndexes(table))
        .map(i -> (Index) convertSecondaryIndex(keyspaceName, tableName(table), i, columns))
        .filter(Objects::nonNull);
  }

  private SecondaryIndex convertSecondaryIndex(
      String keyspaceName, String tableName, I index, List<Column> baseTableColumns) {
    String target = indexTarget(index);
    if (target == null) {
      logger.error(
          "No 'target' defined for index {} on {}.{}. This should not happen and should "
              + "be reported for investigation. That index will not be visible in Stargate schema view",
          indexName(index),
          keyspaceName,
          tableName);
      return null;
    }
    Pair<String, CollectionIndexingType> result = convertTarget(target);
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
    return SecondaryIndex.create(
        keyspaceName,
        indexName(index),
        col.get(),
        result.getValue1(),
        indexClass(index),
        indexOptions(index));
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
  private Pair<String, CollectionIndexingType> convertTarget(String targetColumn) {
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
  private Stream<Index> convertMVIndexes(String keyspaceName, T table, Iterable<V> views) {
    return Streams.of(views)
        .filter(v -> isBaseTableOf(table, v))
        .map(v -> convertMVIndex(keyspaceName, v));
  }

  private Index convertMVIndex(String keyspaceName, V view) {
    T table = asTable(view);
    List<Column> columns = convertColumns(keyspaceName, table).collect(Collectors.toList());
    String comment = comment(table);
    int ttl = ttl(table);
    return MaterializedView.create(keyspaceName, tableName(table), columns, comment, ttl);
  }

  private Stream<UserDefinedType> convertUserTypes(String keyspaceName, Iterable<U> userTypes) {
    return Streams.of(userTypes).map(userType -> convertUserType(keyspaceName, userType));
  }

  private UserDefinedType convertUserType(String keyspaceName, U userType) {
    return ImmutableUserDefinedType.builder()
        .keyspace(keyspaceName)
        .name(userTypeName(userType))
        .columns(userTypeFields(userType))
        .build();
  }
}
